package com.mireau.timeseries;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import com.mireau.timeseries.RawDataSerie.Entry;

/** 
 * Enregistrement des données d'archives consolidées sur un périodicité fixée
 * L'archive est contenu dans un seul fichier
 * 
 * Format:
 * 
 *   En tête commun: (16 bytes)
 *     step      int   / 4 bytes
 *     type      int   / 4 bytes (1:AVERAGE, 2:LAST, 3:CUMUL)
 *     timestamp long  / 8 bytes
 *     
 *   Données sur le step en cours (28 bytes)
 *     stepTimestamp   / 8 bytes : timestamp du step en cours
 *     stepSum   double/ 8 bytes : Somme des valeurs en cours
 *     stepNb    int   / 4 bytes : Nombre de valeurs en cours
 *     stepMin   float / 4 bytes
 *     stepMax   float / 4 bytes
 *     
 *   Enreg AVERAGE: (13 bytes)
 *     Defined  bool  / 1 byte (0:NULL 1:valeur)
 *     Value    float / 4 bytes
 *     Min		float / 4 bytes
 *     Max		float / 4 bytes
 *   Enreg LAST: (5 bytes)
 *     Defined  bool  / 1 byte (0:NULL 1:valeur)
 *     Value    float / 4 bytes
 *   Enreg CUMUL: (5 bytes)
 *     Defined  bool  / 1 byte (0:NULL 1:valeur)
 *     Value    float / 4 bytes
 */
public abstract class ArchiveDataSerie {

	static int HEADER1_LEN = 16;
	
	static int ENREG_LEN_CUMUL = 5;
	
	static int CUR_STEP_RECORD_POS = 16;
	
	static SimpleDateFormat sdf = new SimpleDateFormat("YYYY/MM/dd HH:mm:ss");
	
	enum Type{
		AVERAGE, LAST, CUMUL
	}
	
	/**
	 * Stratégie d'ecriture sur disque
	 * ALL_POINTS (par défaut): 
	 * 		tous les points donnent lieu à une écriture (compteurs sur le step en cours au minimum)
	 * CHANGE_STEP: 
	 * 		ecriture sur changement de step uniquement. 
	 * 		Meilleures performance par moins d'accès disque, mais risque de perte de données sur fin anormale
	 *		
	 */
	enum WriteStrategy{
		ALL_POINTS, CHANGE_STEP
	}
	
	String id;
	WriteStrategy writeStartegy = WriteStrategy.ALL_POINTS;
	
	/** espacement des points dans la série (secondes) */
	Integer step = null;
	
	static Logger logger = Logger.getLogger(ArchiveDataSerie.class.getName());
	
	/** Fichier archive */
	protected File archiveFile;
	
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
	protected ArchiveDataSerie(File file, String id, Integer step) throws IOException, ArchiveInitException{
		this.id = id;
		this.step = step;
		this.archiveFile = file;
		
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
	protected ArchiveDataSerie(File file, String id, Integer step, WriteStrategy strategy) throws IOException, ArchiveInitException{
		this(file,id,step);
		this.writeStartegy = strategy;
	}
	
	/**
	 * Obtient l'instance de l'archive existante
	 * @throws ArchiveInitException 
	 * @throws IOException 
	 */
	public static ArchiveDataSerie getArchive(File file, String id) throws IOException, ArchiveInitException{
		ArchiveDataSerie archive = null;
		if(!file.exists() || file.length()<HEADER1_LEN) throw new ArchiveInitException("Le fichier archive "+file.getAbsolutePath()+" n'existe pas ou est vide");
		RandomAccessFile f = new RandomAccessFile(file,"r");
		int step = f.readInt();
		Type type = decodeType(f.readInt());
		f.close();
		if(type == Type.AVERAGE) archive = new AverageArchiveDataSerie(file, id,step);
		return archive;
	}
	
	/**
	 * Initialise une nouvelle archive.
	 * Création du fichier. Ecriture du premier en-tete
	 * 
	 * @throws IOException
	 * @throws ArchiveInitException
	 */
	protected static void newArchiveFile(Integer step, ArchiveDataSerie.Type type, File file) throws IOException, ArchiveInitException{
		if(step==null) throw new ArchiveInitException("step non initialisé");
		
		/*
		 * Nouveau fichier - On l'initialise ou on écrase
		 */
		//On vérifie que 'step' est cohérent : nombre entier de step dans une heure
		if(step!=60 
			&& step!=   120	//2 min
			&& step!=   300	//5 min
			&& step!=   900	//15 min
			&& step!=  1800	//30 min
			&& step!=  3600	//1 heure
			&& step!=  7200	//2 heure
			&& step!= 14400	//4 heures
			&& step!= 21600	//6 heures
			&& step!= 43200	//12 heures
			&& step!= 86400	//24 heures
			&& step!= 7*86400	//1 semaine
			){ 
			throw new ArchiveInitException("valeur de step incorrecte: "+step);
		}
		
		RandomAccessFile adf = new RandomAccessFile(file, "rw");
		adf.seek(0);
		
		//Step
		adf.writeInt(step);
		
		//Type
		Integer t = ArchiveDataSerie.encodeType(type);
		adf.writeInt(t);
		
		//Timestamp a 0
		adf.writeLong(0);
		
		adf.setLength(adf.getFilePointer());
		
		adf.close();
	}
	
	/**
	 * Obtient l'instance de l'archive (existante ou non)
	 * @throws ArchiveInitException 
	 * @throws IOException 
	 */
	public static ArchiveDataSerie getArchive(File file, String id, Integer step, Type type) throws IOException, ArchiveInitException{
		ArchiveDataSerie archive = null;
		if(type == Type.AVERAGE) archive = new AverageArchiveDataSerie(file, id, step);
		return archive;
	}
	
	
	
	/**
	 * Ouvre et initialise une archive (existante)
	 * 
	 * @param dir
	 * @param id
	 * @param step
	 * @param type
	 * @throws IOException
	 * @throws ArchiveInitException
	 */
	protected synchronized void initArchive() throws IOException, ArchiveInitException{
		logger.info("Init archive : "+archiveFile.getName());
		
		if(!archiveFile.exists() || archiveFile.length()==0) throw new ArchiveInitException("archive does not exists");
		
		long len = archiveFile.length();
		logger.info("archive "+archiveFile.getName()+" len="+len);
		
		/*
		 * Fichier archive existant
		 */
		if(len<HEADER1_LEN){
			//Il faut minimum 8 byte pour l'en tete (step + type)
			throw new ArchiveInitException("Taille du fichier archive incorrecte ("+len+"): inférieure a la taille de l'en-tete");
		}
		
		RandomAccessFile adf = openFile("r");
		
		/*
		 * Step
		 */
		this.step = adf.readInt();
		
		/*
		 * Type
		 */
		int t = adf.readInt();
		if(decodeType(t) != this.type()) throw new ArchiveInitException("incoherence de type d'archive");
		
		/*
		 * Timestamp de debut
		 */
		this.t0 = adf.readLong();
		
		logger.fine("start: "+(t0==0 ? "no start date" : sdf.format(new Date(t0*1000)))+" step:"+step);
		
		/*
		 * Valeurs initialisees : timestamp + valeurs sur le step en cours
		 */
		long nbEnreg = 0;
		this.lastTimestamp = null;
		
		int recordLen = getRecordLen();
		
		if(len > HEADER1_LEN){
			/*
			 * Valeurs sur le step en cours
			 */
			readCurrentStepData(adf);
		}
		
		int firstPos = HEADER1_LEN + currentStepDataLength();
		if(len > firstPos){
			//Il y a des enregistrements
			
			long mod = (len-firstPos) % recordLen;
			if(mod != 0){
				logger.warning("Taille de fichier anormale ("+len+"). Retour a "+(len-mod));
				len -= mod;
			}
			
			//Nb d'enregistrements
			nbEnreg = (len-firstPos) / recordLen;
			
			
			//Calcul du timestamp courant
			this.lastTimestamp = t0 + (nbEnreg-1)*step;
			
			logger.fine(" nb steps: "+nbEnreg);
		}
		
		adf.seek(len);
		
		//On relache le fichier
		releaseFile(adf);
	}
	
	protected RandomAccessFile openFile(String mode) throws FileNotFoundException{
		RandomAccessFile raf;
		raf = new RandomAccessFile(archiveFile, mode);
		return raf;
	}
	
	protected void releaseFile(RandomAccessFile raf) throws IOException{
		if(raf != null) raf.close();
	}
	
	
	/**
	 * Détermine un timestamp d'origine, calé sur le step immédiatement inférieur au timestamp
	 * @param timestamp
	 * @return
	 */
	protected long getTimestampOrigine(long t){
		Calendar cal = GregorianCalendar.getInstance();
		cal.setTimeInMillis(t*1000);
		//On positionne au début de l'heure en cours
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MINUTE, 0);
		if(step>3600){
			//On positionne au début du jour en cours
			cal.set(Calendar.HOUR_OF_DAY, 0);
		}
		if(step>86400){
			//On positionne au début de la semaine
			cal.set(Calendar.DAY_OF_WEEK, 0);
		}
		long result = cal.getTimeInMillis() / 1000;
		
		while(result + step <= t){
			result += step;
		}
		
		return result;
	}
	
	protected static Integer encodeType(Type type){
		if(type == Type.AVERAGE) return 1;
		if(type == Type.LAST) return 2;
		if(type == Type.CUMUL) return 3;
		else return null;
	}
	
	private static Type decodeType(int type){
		switch(type){
			case 1: return Type.AVERAGE;
			case 2: return Type.LAST;
			case 3: return Type.CUMUL;
			default: return null;
		}
	}
	
	public Type type(){
		if(this instanceof AverageArchiveDataSerie) return Type.AVERAGE;
		else return null;
	}
	
	/**
	 * Ferme une archive en enregistrant les valeurs reçues sur le step en cours
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException{
		RandomAccessFile raf = openFile("rw");
		//writeCurrentStepData(raf);
		releaseFile(raf);
	}
	
	/**
	 * Ecriture des données sur le step en cours
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
	 * @throws IOException 
	 */
	protected abstract ArchivePoint readPoint(RandomAccessFile raf) throws IOException;
	
	/**
	 * Retourne la taille d'un enregistrement dans le fichier
	 */
	protected abstract int getRecordLen();
	
	/**
	 * Enregistrement d'un point
	 * @param timestamp
	 * @param value
	 * @throws IOException
	 * @throws ArchiveInitException 
	 */
	public abstract void post(long timestamp, float value) throws IOException, ArchiveInitException;
	
	/**
	 * Construit l'objet ArchivePoint correspondant aux valeurs enregistrées sur le step en cours
	 */
	protected abstract ArchivePoint currentStepPoint();
	
	/**
	 * @throws ArchiveInitException 
	 * @throws IOException 
	 * 
	 */
	public void build(Iterator<Entry> iter) throws IOException, ArchiveInitException{
		//On tronque le fichier au timestamp correspondant a la premiere valeur.
		if(!iter.hasNext()) return;
		Entry e = iter.next();
		if(t0!=null && t0>0){
			//l'archive existe déjà avec un timestamp de début défini
			if(lastTimestamp==null || t0>lastTimestamp){
				//Suppression du fichier
				archiveFile.delete();
			}
			else if(e.timestamp < lastTimestamp + getRecordLen()){
				//timestamp dans l'archive ou anterieur : on tronque
				Long pos = getTimestampPosition(e.timestamp);
				logger.info("troncature du fichier archive "+archiveFile.getName()+" à pos="+pos);
				RandomAccessFile raf = openFile("rw");
				raf.setLength(pos);
				resetCurrentStepData();
				this.writeCurrentStepData(raf);
				if(pos > HEADER1_LEN + currentStepDataLength()){
					raf.seek(pos-getRecordLen());
					ArchivePoint p = readPoint(raf);
					lastTimestamp = p.timestamp;
				}
				releaseFile(raf);
			}
		}
		
		//On poste la premiere valeur deja recuperee
		this.post(e.timestamp, e.value);
		
		//Ajout des valeurs
		while(iter.hasNext()){
			e = iter.next();
			this.post(e.timestamp, e.value);
		}
	}
	
	/**
	 * Positionne le curseur sur la valeur correspondante au timestamp (exact ou suivant)
	 * @param timestamp
	 * @return position
	 * @throws IOException 
	 */
	private Long getTimestampPosition(Long timestamp){
		long nbToStart = 0;
		if(timestamp!=null && timestamp > t0){
			nbToStart = (timestamp - t0) / step;
		}
		long startIdx = nbToStart*getRecordLen() + HEADER1_LEN + currentStepDataLength();
		return startIdx;
	}
	
	
	/**
	 * Recherche d'une serie de point
	 * @throws IOException 
	 */
	public List<ArchivePoint> getPoints(Long startTimestamp, Long endTimestamp) throws IOException{
		if(startTimestamp==null) startTimestamp = (long)0;
		
		List<ArchivePoint> result = new ArrayList<ArchivePoint>();
		
		long len = this.archiveFile.length();
		
		//Positionnement sur la premiere valeur
		Long startIdx = getTimestampPosition(startTimestamp);
		
		//Fin
		if(endTimestamp == null || endTimestamp > lastTimestamp) endTimestamp = lastTimestamp;
		
		if(startIdx < len){
			RandomAccessFile raf = openFile("r");
			raf.seek(startIdx);
			for(long t=startTimestamp;t<endTimestamp;t+=step){
				startIdx+=getRecordLen();
				System.out.println("pos="+startIdx);
				
				ArchivePoint point = readPoint(raf);
				point.timestamp = t;
				result.add(point);
			}
			releaseFile(raf);
		}
		
		if(endTimestamp==null || lastTimestamp==null || endTimestamp >= lastTimestamp){
			//Ajout de la valeur en cours (non enregistree dans le fichier
			ArchivePoint point = this.currentStepPoint();
			if(point!=null && point.value!=null) result.add(point);
		}
		
		return result;
	}
	
	public List<ArchivePoint> getLastPoints(int nb) throws IOException{
		Long startTimestamp = null;
		if(this.lastTimestamp != null){
			if(this.currentStepPoint()!=null && nb>0) nb--;
			startTimestamp = this.lastTimestamp - nb*this.step;
			if(startTimestamp<this.t0) startTimestamp = this.t0;
		}
		
		return getPoints(startTimestamp,null);
	}


	public WriteStrategy getWriteStartegy() {
		return writeStartegy;
	}

	public void setWriteStartegy(WriteStrategy writeStartegy) {
		this.writeStartegy = writeStartegy;
	}
}