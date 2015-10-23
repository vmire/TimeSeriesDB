package com.mireau.homeAutomation.timeseries;

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

import com.mireau.homeAutomation.timeseries.RawDataSerie.Entry;

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
	
	
	String id;
	
	/** espacement des points dans la série (secondes) */
	Integer step = null;
	
	static Logger logger = Logger.getLogger(ArchiveDataSerie.class.getName());
	
	/** Fichier archive */
	File archiveFile;
	
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
	 * Initialise une nouvelle archive.
	 * Création du fichier. Ecriture du premier en-tete
	 * 
	 * @throws IOException
	 * @throws ArchiveInitException
	 */
	protected synchronized void newArchive() throws IOException, ArchiveInitException{
		if(step==null) throw new ArchiveInitException("step non initialisé");
		
		/*
		 * Nouveau fichier - On l'initialise
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
		
		RandomAccessFile adf = openFile("rw");
		
		//Step
		adf.writeInt(step);
		
		//Type
		Integer t = encodeType();
		adf.writeInt(t);
		
		//Timestamp à 0
		adf.writeLong(0);
		
		
		adf.setLength(adf.getFilePointer());
		
		releaseFile(adf);
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
		
		if(!archiveFile.exists() || archiveFile.length()==0){
			//Le fichier archive n'existe pas. On la crée
			newArchive();
			return;
		}
		
		long len = archiveFile.length();
		logger.info("archive "+archiveFile.getName()+" len="+len);
		
		/*
		 * Fichier archive existant
		 */
		if(len<HEADER1_LEN){
			//Il faut minimum 8 byte pour l'en tete (step + type)
			throw new ArchiveInitException("Taille du fichier archive incorrecte ("+len+"): inférieure à la taille de l'en-tête");
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
		if(decodeType(t) != this.type()) throw new ArchiveInitException("incohérence de type d'archive");
		
		/*
		 * Timestamp de début
		 */
		this.t0 = adf.readLong();
		
		logger.fine("start: "+sdf.format(new Date(t0*1000))+" step:"+step);
		
		/*
		 * Valeurs initialisées : timestamp + valeurs sur le step en cours
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
				logger.warning("Taille de fichier anormale ("+len+"). Retour à "+(len-mod));
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
		raf.close();
	}
	
	
	/**
	 * Détermine un timestamp d'origine, calé sur une valeur "ronde" en fonction du step
	 * @param timestamp
	 * @return
	 */
	protected long getTimestampOrigine(long t){
		//Choix d'un timestamp origine en fonction du step
		Calendar cal = GregorianCalendar.getInstance();
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
		return cal.getTimeInMillis() / 1000;
	}
	
	private Integer encodeType(){
		if(this instanceof AverageArchiveDataSerie) return 1;
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
		writeCurrentStepData(raf);
		releaseFile(raf);
	}
	
	/**
	 * Ecriture des données sur le step en cours
	 */
	protected abstract void writeCurrentStepData(RandomAccessFile raf) throws IOException;
	
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
	 */
	public abstract void post(long timestamp, float value) throws IOException;
	
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
		//On tronque le fichier au timestamp correspondant à la première valeur.
		if(!iter.hasNext()) return;
		Entry e = iter.next();
		if(t0!=null && t0>0){
			if(e.timestamp < lastTimestamp + getRecordLen()){
				//timestamp dans l'archive ou antérieur : on tronque
				Long pos = getTimestampPosition(e.timestamp);
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
		
		//On poste la première valeur déja récupérée
		this.post(e.timestamp, e.value);
		
		//Ajout des valeurs
		while(iter.hasNext()){
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
		List<ArchivePoint> result = new ArrayList<ArchivePoint>();
		
		long len = this.archiveFile.length();
		
		//Positionnement sur la première valeur
		Long startIdx = getTimestampPosition(startTimestamp);
		
		//Fin
		if(endTimestamp == null || endTimestamp > lastTimestamp) endTimestamp = lastTimestamp;
		
		if(startIdx < len){
			RandomAccessFile raf = openFile("r");
			raf.seek(startIdx);
			for(long t=startTimestamp;t<endTimestamp;t+=step){
				ArchivePoint point = readPoint(raf);
				point.timestamp = t;
				result.add(point);
			}
			releaseFile(raf);
		}
		
		if(endTimestamp==null || lastTimestamp==null || endTimestamp >= lastTimestamp){
			//Ajout de la valeur en cours (non enregistrée dans le fichier
			ArchivePoint point = this.currentStepPoint();
			if(point!=null && point.value!=null) result.add(point);
		}
		
		return result;
	}
	
	
	
	
	/**
	 * Classe qui représente un point de l'archive
	 */
	public class ArchivePoint{
		Float value = null;
		long timestamp;
		public Float getValue(){ return value; }
	}
}
