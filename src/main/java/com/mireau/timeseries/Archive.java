package com.mireau.timeseries;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import com.mireau.timeseries.RawData.Entry;

/**
 * Enregistrement des données d'archives consolidées sur un périodicité fixée
 * L'archive est contenu dans un seul fichier
 * 
 * Format:
 * 
 * En tête commun: (16 bytes) 
 *  step int / 4 bytes 
 *  type int / 4 bytes (1:AVERAGE, 2:ABS_COUNTER, 3:REL_COUNTER) 
 *  timestamp long / 8 bytes
 * 
 * Type AVERAGE: (+13 bytes = 44bytes) 
 *  Description: Pour enregistrer des valeurs telles que des température Similaire au type GAUGE de RRDTool En-tête :
 *  Données sur le step en cours (28 bytes) 
 *   stepTimestamp / 8 bytes : timestamp du step en cours 
 *   stepSum double/ 8 bytes : Somme des valeurs en cours 
 *   stepNb int / 4 bytes : Nombre de valeurs en cours 
 *   stepMin float / 4 bytes 
 *   stepMax float / 4 bytes 
 *  
 *  Enregistrements: (13 bytes) 
 *   Defined bool / 1 byte (0:NULL 1:valeur) 
 *   Value float / 4 bytes 
 *   Min float / 4 bytes 
 *   Max float / 4 bytes
 * 
 * Type ABS_COUNTER: Description: Enregistrement de compteurs en valeur absolue
 * (ex: compteur électrique) Part du principe que la valeur est uniquement
 * croissante, sauf en cas d'overflow où le compteur est remis à 0. Similaire au
 * type COUNTER de RRDTool Enregistrements: (5 bytes) Defined bool / 1 byte
 * (0:NULL 1:valeur) Value float / 4 bytes
 * 
 * Type REL_COUNTER: Description: Enregistrement de compteurs en valeur relative
 * (impulsions depuis le dernier enregistrement) Enregistrements: (5 bytes)
 * Defined bool / 1 byte (0:NULL 1:valeur) Value float / 4 bytes
 */
public abstract class Archive {

	static int HEADER1_LEN = 16;

	static int ENREG_LEN_CUMUL = 5;

	static int CUR_STEP_RECORD_POS = 16;

	static SimpleDateFormat sdf = new SimpleDateFormat("YYYY/MM/dd HH:mm:ss");

	public enum Type {
		AVERAGE, ABS_COUNTER, REL_COUNTER
	}

	/**
	 * 60 //1min 
	 * 120 //2 min 
	 * 300 //5 min 
	 * 900 //15 min 
	 * 1800 //30 min 
	 * 3600 //1 heure 
	 * 7200 //2 heure 
	 * 14400 //4 heures 
	 * 21600 //6 heures 
	 * 43200 //12 heures
	 * 86400 //24 heures 
	 * 7*86400 //1 semaine
	 */
	public static final int[] ALLOWED_STEPS = { 60, 120, 300, 900, 1800, 3600, 7200, 14400, 21600, 43200, 86400,
			7 * 86400 };

	/**
	 * Stratégie d'ecriture sur disque ALL_POINTS (par défaut): tous les points
	 * donnent lieu à une écriture (compteurs sur le step en cours au minimum)
	 * CHANGE_STEP: ecriture sur changement de step uniquement. Meilleures
	 * performance par moins d'accès disque, mais risque de perte de données sur
	 * fin anormale
	 * 
	 */
	enum WriteStrategy {
		ALL_POINTS, CHANGE_STEP
	}
	
	String id;
	WriteStrategy writeStartegy = WriteStrategy.ALL_POINTS;

	/** espacement des points dans la série (secondes) */
	Integer step = null;

	static Logger logger = Logger.getLogger(Archive.class.getName());

	/** Fichier archive */
	protected File archiveFile;

	/** Lock sur l'archive */
	ReadWriteLock lock;

	/**
	 * timestamp du dernier step enregistré (terminé)
	 */
	Long t0 = null;
	Long lastTimestamp = null;

	/**
	 * Constructeur d'une nouvelle archive
	 * 
	 * @param dir
	 * @param id
	 * @param step
	 * @throws IOException
	 * @throws ArchiveInitException
	 */
	protected Archive(File file, String id, Integer step) throws IOException, ArchiveInitException {
		this.id = id;
		this.step = step;
		this.archiveFile = file;

		this.lock = new ReentrantReadWriteLock();
		initArchive();
	}

	/**
	 * Constructeur d'une nouvelle archive
	 * 
	 * @param dir
	 * @param id
	 * @param step
	 * @param writeStrategy
	 * @throws IOException
	 * @throws ArchiveInitException
	 */
	protected Archive(File file, String id, Integer step, WriteStrategy strategy)
			throws IOException, ArchiveInitException {
		this(file, id, step);
		this.writeStartegy = strategy;
	}

	/**
	 * Obtient l'instance de l'archive existante
	 * 
	 * @throws ArchiveInitException
	 * @throws IOException
	 */
	public static Archive getArchive(File file, String id) throws IOException, ArchiveInitException {
		Archive archive = null;
		if (!file.exists() || file.length() < HEADER1_LEN)
			throw new ArchiveInitException(
					"Le fichier archive " + file.getAbsolutePath() + " n'existe pas ou est vide");
		RandomAccessFile f = new RandomAccessFile(file, "r");
		int step = f.readInt();
		Type type = decodeType(f.readInt());
		f.close();
		if (type == Type.AVERAGE)
			archive = new AverageArchive(file, id, step);
		return archive;
	}

	/**
	 * Initialise une nouvelle archive. Création du fichier. Ecriture du premier
	 * en-tete
	 * 
	 * @throws IOException
	 * @throws ArchiveInitException
	 */
	protected static void newArchiveFile(Integer step, Archive.Type type, File file)
			throws IOException, ArchiveInitException {
		if (step == null)
			throw new ArchiveInitException("step non initialisé");

		/*
		 * Nouveau fichier - On l'initialise ou on écrase
		 */
		// On vérifie que 'step' est cohérent : nombre entier de step dans une heure
		if (Arrays.binarySearch(ALLOWED_STEPS, step) < 0) {
			throw new ArchiveInitException("valeur de step incorrecte: " + step);
		}

		RandomAccessFile adf = new RandomAccessFile(file, "rw");
		adf.seek(0);

		// Step
		adf.writeInt(step);

		// Type
		Integer t = Archive.encodeType(type);
		adf.writeInt(t);

		// Timestamp a 0
		adf.writeLong(0);

		adf.setLength(adf.getFilePointer());

		adf.close();
	}

	/**
	 * Obtient l'instance de l'archive (existante ou non)
	 * 
	 * @throws ArchiveInitException
	 * @throws IOException
	 */
	public static Archive getArchive(File file, String id, Integer step, Type type)
			throws IOException, ArchiveInitException {
		Archive archive = null;
		if (type == Type.AVERAGE)
			archive = new AverageArchive(file, id, step);
		return archive;
	}

	/**
	 * Initialise l'objet archive avec le contenu du fichier correspondant
	 * 
	 * @throws IOException
	 * @throws ArchiveInitException
	 */
	protected void initArchive() throws IOException, ArchiveInitException {
		logger.info("Init archive : " + archiveFile.getName());

		if (!archiveFile.exists() || archiveFile.length() == 0)
			throw new ArchiveInitException("archive does not exists");
		
		RandomAccessFile adf = null;
		
		//writeLock
		//On lit le fichier, mais on modifie l'objet. C'est donc un writeLock
		lock.writeLock().lock();
		try{
			long len = archiveFile.length();
			logger.info("archive " + archiveFile.getName() + " len=" + len);
	
			/*
			 * Fichier archive existant
			 */
			if (len < HEADER1_LEN) {
				// Il faut minimum 8 byte pour l'en tete (step + type)
				throw new ArchiveInitException(
						"Taille du fichier archive incorrecte (" + len + "): inférieure a la taille de l'en-tete");
			}
			
			adf = openFileForReading();
	
			/*
			 * Step
			 */
			this.step = adf.readInt();
	
			/*
			 * Type
			 */
			int t = adf.readInt();
			if (decodeType(t) != this.getType())
				throw new ArchiveInitException("incoherence de type d'archive");
	
			/*
			 * Timestamp de debut
			 */
			this.t0 = adf.readLong();
	
			logger.fine("start: " + (t0 == 0 ? "no start date" : sdf.format(new Date(t0 * 1000))) + " step:" + step);
	
			/*
			 * Valeurs initialisees : timestamp + valeurs sur le step en cours
			 */
			long nbEnreg = 0;
			this.lastTimestamp = null;
	
			int recordLen = getRecordLen();
	
			if (len > HEADER1_LEN) {
				/*
				 * Valeurs sur le step en cours
				 */
				readCurrentStepData(adf);
			}
	
			int firstPos = HEADER1_LEN + currentStepDataLength();
			if (len > firstPos) {
				// Il y a des enregistrements
	
				long mod = (len - firstPos) % recordLen;
				if (mod != 0) {
					logger.warning("Taille de fichier anormale (" + len + "). Retour a " + (len - mod));
					len -= mod;
					adf.close(); // Il est en mode lecture uniquement
					adf = openFileForWriting(false);
					adf.setLength(len);
					
					// TODO : c'est quand même bizarre. corruption d'archive
					// probable. il faudrait reconstruire
				}
	
				// Nb d'enregistrements
				nbEnreg = (len - firstPos) / recordLen;
	
				// Calcul du timestamp courant
				this.lastTimestamp = t0 + (nbEnreg - 1) * step;
	
				logger.fine(" nb steps: " + nbEnreg);
			}
		}
		finally{
			// On relache le fichier
			if(adf!=null) adf.close();
			releaseFile();
			lock.writeLock().unlock();
		}
	}
	
	protected void releaseFile() throws IOException{
		//if(lock!=null) lock.release();
	}

	protected RandomAccessFile openFileForReading() throws IOException {
		RandomAccessFile raf;
		raf = new RandomAccessFile(archiveFile, "r");
		return raf;
	}

	protected RandomAccessFile openFileForWriting(boolean wait) throws IOException {
		RandomAccessFile raf;
		raf = new RandomAccessFile(archiveFile, "rw");
		/*
		FileChannel channel = raf.getChannel();
		if(wait){
			lock = channel.lock();
		}
		else{
			lock = channel.tryLock();
		}
		*/
		return raf;
	}
	
	/**
	 * Détermine un timestamp d'origine, calé sur le step immédiatement
	 * inférieur au timestamp
	 * 
	 * @param timestamp
	 * @return
	 */
	protected long getTimestampOrigine(long t) {
		Calendar cal = GregorianCalendar.getInstance();
		cal.setTimeInMillis(t * 1000);
		// On positionne au début de l'heure en cours
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MINUTE, 0);
		if (step > 3600) {
			// On positionne au début du jour en cours
			cal.set(Calendar.HOUR_OF_DAY, 0);
		}
		if (step > 86400) {
			// On positionne au début de la semaine
			cal.set(Calendar.DAY_OF_WEEK, 0);
		}
		long result = cal.getTimeInMillis() / 1000;

		while (result + step <= t) {
			result += step;
		}

		return result;
	}

	protected static Integer encodeType(Type type) {
		if (type == Type.AVERAGE)
			return 1;
		if (type == Type.ABS_COUNTER)
			return 2;
		if (type == Type.REL_COUNTER)
			return 3;
		else
			return null;
	}

	private static Type decodeType(int type) {
		switch (type) {
		case 1:
			return Type.AVERAGE;
		case 2:
			return Type.ABS_COUNTER;
		case 3:
			return Type.REL_COUNTER;
		default:
			return null;
		}
	}

	public static Type decodeType(String type) {
		if("AVERAGE".equalsIgnoreCase(type)) return Type.AVERAGE;
		if("ABS_COUNTER".equalsIgnoreCase(type)) return Type.ABS_COUNTER;
		if("REL_COUNTER".equalsIgnoreCase(type)) return Type.REL_COUNTER;
		return null;
	}
	
	public Type getType() {
		if (this instanceof AverageArchive)
			return Type.AVERAGE;
		else
			return null;
	}

	/**
	 * Ferme une archive en enregistrant les valeurs reçues sur le step en cours
	 * 
	 * @throws IOException
	 * @throws ArchiveInitException
	 */
	public void close() throws IOException, ArchiveInitException {
		if (writeStartegy == WriteStrategy.CHANGE_STEP) {
			RandomAccessFile raf = null;
			try{
				this.lock.writeLock().lock();
				raf = openFileForWriting(true);
				writeCurrentStepData(raf);
			}
			finally{
				releaseFile();
				if(raf!=null) raf.close();
				lock.writeLock().unlock();
			}
		}
	}

	/**
	 * Ecriture des données sur le step en cours
	 * 
	 * @throws ArchiveInitException
	 */
	protected abstract void writeCurrentStepData(RandomAccessFile raf) throws IOException, ArchiveInitException;

	/**
	 * reinitialisation des variables sur le step en cours
	 */
	protected abstract void resetCurrentStepData();

	/**
	 * Lecture des données sur le step en cours
	 */
	protected abstract void readCurrentStepData(RandomAccessFile adf) throws IOException;

	/**
	 * Longueur des données du step en cours écrites dans le fichier archive
	 */
	protected abstract int currentStepDataLength();

	/**
	 * Lit le point dans le fichier, à la position du curseur
	 * 
	 * @throws IOException
	 */
	protected abstract ArchivePoint readPoint(RandomAccessFile raf) throws IOException;

	/**
	 * Retourne la taille d'un enregistrement dans le fichier
	 */
	protected abstract int getRecordLen();

	/**
	 * Enregistrement d'un point
	 * 
	 * @param timestamp
	 * @param value
	 * @throws IOException
	 * @throws ArchiveInitException
	 */
	public abstract void post(long timestamp, float value) throws IOException, ArchiveInitException;

	protected abstract void post(long timestamp, float value, RandomAccessFile adf)
			throws IOException, ArchiveInitException;

	/**
	 * Construit l'objet ArchivePoint correspondant aux valeurs enregistrées sur
	 * le step en cours
	 */
	protected abstract ArchivePoint currentStepPoint();

	/**
	 * @throws ArchiveInitException
	 * @throws IOException
	 * 
	 */
	public void build(Iterator<Entry> iter) throws IOException, ArchiveInitException {
		// On tronque le fichier au timestamp correspondant a la premiere
		// valeur.
		if (!iter.hasNext())
			return;
		RandomAccessFile raf = null;
		lock.writeLock().lock();
		try {
			Entry e = iter.next();
			raf = openFileForWriting(false);
			if (t0 != null) {
				// l'archive existe déjà avec un timestamp de début défini
				if (lastTimestamp == null || e.timestamp < lastTimestamp + getRecordLen()) {
					// timestamp dans l'archive ou anterieur :
					// on tronque l'archive à la position de la première valeur
					// de la série à insérer
					Long pos = getTimestampPosition(e.timestamp);
					logger.info("troncature du fichier archive " + archiveFile.getName() + " à pos=" + pos);
					raf.setLength(pos);
					resetCurrentStepData();

					if (pos > HEADER1_LEN + currentStepDataLength()) {
						// On conserve des points dans l'archive. on va lire la
						// dernière valeur
						raf.seek(pos - getRecordLen());
						ArchivePoint p = readPoint(raf);
						lastTimestamp = p.timestamp;
					}
				}
			}

			// on désactive l'écriture systématique (du current step) à chaque
			// point
			WriteStrategy writeStrategy = this.writeStartegy;
			this.setWriteStartegy(WriteStrategy.CHANGE_STEP);

			// On poste la premiere valeur deja recuperee
			this.post(e.timestamp, e.value, raf);

			// Ajout des valeurs
			while (iter.hasNext()) {
				e = iter.next();
				this.post(e.timestamp, e.value, raf);
			}

			// ecriture du current step
			this.writeCurrentStepData(raf);

			// On rétabli la valeur de WriteStrategy
			this.setWriteStartegy(writeStrategy);
		} finally {
			if (raf != null)
				raf.close();
			releaseFile();
			lock.writeLock().unlock();
		}
	}

	/**
	 * Positionne le curseur sur la valeur correspondante au timestamp (exact ou
	 * suivant)
	 * 
	 * @param timestamp
	 * @return position
	 * @throws IOException
	 */
	private Long getTimestampPosition(Long timestamp) {
		long nbToStart = 0;
		if (t0 > 0 && timestamp != null && timestamp > t0) {
			// si t0==0, il n'est en fait pas défini
			nbToStart = (timestamp - t0) / step;
		}
		long startIdx = nbToStart * getRecordLen() + HEADER1_LEN + currentStepDataLength();
		return startIdx;
	}

	protected ArchivePoint newEmptyPoint(Long timestamp) {
		ArchivePoint p = new ArchivePoint();
		p.timestamp = timestamp;
		return p;
	}

	/**
	 * Recherche d'une serie de point
	 * Attend indéfiniment le readLock
	 * 
	 * @param startTimestamp
	 * @param endTimestamp
	 * @param timeoutMillis
	 * @return List<ArchivePoint>
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public List<ArchivePoint> getPoints(Long startTimestamp, Long endTimestamp) throws IOException, InterruptedException {
		try {
			return getPoints(startTimestamp, endTimestamp,-1);
		} catch (LockTimeoutException e) {
			//Ca ne devrait pas arriver
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Recherche d'une serie de point
	 * Si le readLock ne peut être obtenu dans le délai spécifié, une LockTimeoutException est levée
	 * 
	 * @param startTimestamp
	 * @param endTimestamp
	 * @param timeoutMillis
	 * @return List<ArchivePoint>
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws LockTimeoutException 
	 */
	public List<ArchivePoint> getPoints(Long startTimestamp, Long endTimestamp, int timeoutMillis) throws IOException, InterruptedException, LockTimeoutException {
		if (startTimestamp == null)
			startTimestamp = (long) 0;
		List<ArchivePoint> result = new ArrayList<ArchivePoint>();

		if (startTimestamp != null && endTimestamp != null && startTimestamp >= endTimestamp)
			return result;

		// On cale startTimestamp sur les valeurs de l'archive
		startTimestamp -= (startTimestamp - t0) % step;

		long len = this.archiveFile.length();

		// Positionnement sur la premiere valeur
		Long startIdx = getTimestampPosition(startTimestamp);

		// Fin
		if (endTimestamp == null)
			endTimestamp = (new Date()).getTime() / 1000;

		// currrentStep
		ArchivePoint curStepPoint = this.currentStepPoint();

		RandomAccessFile raf = null;
		
		//Read Lock
		if(!lock.readLock().tryLock(timeoutMillis,TimeUnit.MILLISECONDS)){
			throw new LockTimeoutException(timeoutMillis+" timeout expired");
		}
		
		try{
			long cursorTimestamp = startTimestamp;
			long cursorIdx = startIdx;
			while (cursorTimestamp < endTimestamp) {
				ArchivePoint point = null;
				if (cursorIdx > len - getRecordLen()) {
					// le curseur n'est pas dans le fichier. Cause possible :
					// absence de valeurs enregistrée à la fin
					if (cursorTimestamp == curStepPoint.timestamp)
						point = curStepPoint;
					else if (cursorTimestamp > curStepPoint.timestamp) {
						// On est au dela de curStep : on arrête sans mettre de
						// point vides
						break;
					} else
						point = newEmptyPoint(cursorTimestamp);
				} else {
					// Ouverture du fichier si besoin
					if (raf == null)
						raf = openFileForReading();
					// Postionnement dans le fichier si besoin
					if (raf.getChannel().position() != cursorIdx)
						raf.seek(cursorIdx);
	
					// On lit le point dans le fichier
					point = readPoint(raf);
				}
	
				point.timestamp = cursorTimestamp;
				result.add(point);
				cursorTimestamp += step;
				cursorIdx += getRecordLen();
	
			}
		}
		finally{
			if (raf != null)
				raf.close();
			lock.readLock().unlock();
		}
		return result;
	}

	public List<ArchivePoint> getLastPoints(int nb) throws IOException, InterruptedException {
		Long startTimestamp = null;
		if (nb == 0)
			return new ArrayList<ArchivePoint>();

		ArchivePoint curStepPoint = currentStepPoint();

		if (curStepPoint != null) {
			nb--;
			startTimestamp = curStepPoint.timestamp - nb * this.step;
		} else if (this.lastTimestamp != null) {
			startTimestamp = this.lastTimestamp - nb * this.step;
		}
		if (startTimestamp < this.t0)
			startTimestamp = this.t0;

		return getPoints(startTimestamp, null);
	}

	public WriteStrategy getWriteStartegy() {
		return writeStartegy;
	}

	public void setWriteStartegy(WriteStrategy writeStartegy) {
		this.writeStartegy = writeStartegy;
	}

	public String getId() {
		return id;
	}

	public Integer getStep() {
		return step;
	}

	public Date getT0() {
		return (t0 == null ? null : new Date(t0 * 1000));
	}

	public Long getLastTimestamp() {
		return lastTimestamp;
	}
	
	public long getFileSize(){
		return archiveFile.length();
	}
}
