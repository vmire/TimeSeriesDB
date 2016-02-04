package com.mireau.timeseries;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Date;

/**
 * Enregistrement de compteurs en valeur absolue (ex: compteur électrique)
 * Part du principe que la valeur est uniquement croissante, sauf en cas d'overflow où le compteur est remis à 0.
 * Similaire au type COUNTER de RRDTool
 * 
 *      En-tête spécifique : (16 bytes)
 *        stepTimestamp  / 8 bytes : timestamp du step en cours
 *        stepNb   int   / 4 bytes : nb de valeurs brutes constituant le step en cours
 *        stepMax  float / 4 bytes : valeur max du step en cours
 *      Enregistrements: (9 bytes)
 *        Flags             1 byte (Most Significant Bit first)
 *            0 : defined (0:NULL 1:valeur)
 *            1 : overflow
 *        Value    float / 4 bytes
 *        Diff     float / 4 bytes  (différence avec le step précédent)
 */
public class AbsCounterArchive extends Archive {

	static int ENREG_LEN = 9;
	static int CURRENT_STEP_DATA_LENGTH = 16;
	
	
	/** nombre de valeurs pour le step en cours */
	int stepNb;
	/** dernière valeur sur le step en cours*/
	Float stepCounter;
	/** valeur maxi sur le step en cours (utile en cas d'overflow) */
	Float stepMax;
	/** timestamp du debut de step en cours */
	Long stepTimestamp;
	
	/** Dernier point connu de l'archive (pour calcul du diff) */
	AbsCounterArchivePoint previousPoint;
	
	
	public AbsCounterArchive(File file, String id, Integer step) throws IOException, ArchiveInitException {
		super(file, id, step);
	}

	/**
	 * Retourne la taille d'un enregistrement dans le fichier
	 */
	protected  int getRecordLen(){
		return ENREG_LEN;
	}
	
	/**
	 * Enregistrement des données sur le step en cours
	 * @throws IOException 
	 * @throws ArchiveInitException 
	 */
	protected synchronized void writeCurrentStepData(RandomAccessFile raf) throws IOException, ArchiveInitException{
		if(stepTimestamp==null) throw new ArchiveInitException("stepTimestamp non initialisé");
		logger.fine("write current step vars : "+sdf.format(new Date((long)stepTimestamp*1000))+" nb="+stepNb+" max="+stepMax);
		raf.seek(CUR_STEP_RECORD_POS);
		raf.writeLong(stepTimestamp);
		raf.writeInt(stepNb);
		raf.writeFloat(stepMax==null ? 0 : stepMax);
	}
	
	/**
	 * reinitialisation des variables sur le step en cours
	 */
	@Override
	protected void resetCurrentStepData(){
		stepTimestamp = null;
		stepNb = 0;
		stepMax = null;
	} 
	/**
	 * Lecture des données sur le step en cours
	 */
	protected void readCurrentStepData(RandomAccessFile adf) throws IOException{
		adf.seek(CUR_STEP_RECORD_POS);
		
		stepTimestamp = adf.readLong();
		stepNb = adf.readInt();
		stepMax = adf.readFloat();
		
		if(stepNb==0) stepMax = null;
		
		//Lecture du dernier point
		long pos = adf.length() - this.enregLen();
		adf.seek(pos);
		this.previousPoint = (AbsCounterArchivePoint)this.readPoint(adf);
		if(previousPoint!=null && previousPoint.value==null) previousPoint = null;
		
		logger.fine("step en cours: nb="+stepNb+" max="+stepMax+" timestamp="+stepTimestamp);
	}
	
	/**
	 * Longueur des données du step en cours écrites dans le fichier archive
	 */
	protected int currentStepDataLength(){
		return CURRENT_STEP_DATA_LENGTH;
	}
	
	/**
	 * Longueur d'un enregistrement de l'archive
	 * @return
	 */
	@Override
	protected int enregLen(){
		return ENREG_LEN;
	}
	
	/**
	 * Construit l'objet AbsCounterPoint correspondant aux valeurs enregistrées sur le step en cours
	 */
	protected ArchivePoint currentStepPoint(){
		if(this.stepNb==0) return null;
		AbsCounterArchivePoint point = new AbsCounterArchivePoint();
		point.value = this.stepCounter;
		point.overflow = false;
		point.timestamp = this.stepTimestamp;
		
		if(point.value==null) return null;
		
		if(this.previousPoint==null || previousPoint.value==null){
			//Pas de point précédent : c'est le premier point de l'archive
			logger.warning("unknown previous point");
			point.diff = (float) 0;
			point.overflow = true;
		}
		else{
			if(stepMax>=previousPoint.value){
				point.diff = stepMax - previousPoint.value;
				if(stepMax>stepCounter){
					//Il y a eu un overflow du compteur
					point.diff += stepCounter;
					point.overflow = true;
				}
			}
			else{
				//étrange : il peut éventuellement y avoir eu overflow avant la première valeur brute du step
				logger.warning("stepMax("+stepMax+") < previousPoint("+previousPoint.value+")");
				point.overflow = true;
				point.diff = stepCounter;
			}
		}
		if(point.value==null){
			logger.warning("null point.value");
		}
		return point;
	}
	
	
	
	/**
	 * Enregistrement d'un point
	 * Le curseur doit être positionné en fin de fichier
	 * @param timestamp
	 * @param value
	 * @param ramdomAccessFile 
	 * @throws IOException
	 * @throws ArchiveInitException 
	 */
	@Override
	protected void post(long timestamp, float value, RandomAccessFile adf) throws IOException, ArchiveInitException{
		boolean keepFileOpened = adf!=null;
		lock.writeLock().lock();
		
		try{
			if(stepTimestamp!=null && stepTimestamp>0 && timestamp < stepTimestamp){
				logger.warning("Nouvelle valeur anterieure au step en cours (cur step:"+sdf.format(new Date(stepTimestamp*1000))+" new value:"+sdf.format(new Date(timestamp*1000))+")");
			}
			
			if(stepTimestamp==null || stepTimestamp==0){
				//Calcul du timestamp d'origine de l'archive : arrondi au step immédiatement inférieur
				stepTimestamp = getTimestampOrigine(timestamp);
				this.t0 = stepTimestamp;
				logger.fine("write start timestamp");
				//ecriture du timestamp dans l'en-tête
				if(adf==null) adf = openFileForWriting(true);
				adf.seek(8);	//on se positionne sur le champ timestamp de début
				adf.writeLong(stepTimestamp);
			}
			else if(timestamp >= stepTimestamp+step){
				/*
				 * Changement de step -> écriture
				 */	
				if(adf==null) adf = openFileForWriting(true);
				this.writePoint(adf);
				
				stepNb = 0;
				stepMax = this.stepCounter;	//Le nouveau max (pour le step suivant) est la valeur courante
				
				long nbSteps = (timestamp - lastTimestamp)/step;
				this.stepTimestamp = this.lastTimestamp + nbSteps*step;
				logger.fine("nouveau step : "+sdf.format(new Date(stepTimestamp*1000)));
			}
			
			this.stepCounter = value;
			this.stepNb++;
			if(this.stepMax==null || value > this.stepMax) this.stepMax = value;
			
			logger.fine("add to current step : "+value+" nb="+stepNb+" max="+stepMax);
			
			if(this.writeStartegy == WriteStrategy.ALL_POINTS){
				//On écrit systématiquement le données du step en cours
				//Plus sûr, mais moins performant
				if(adf==null) adf = openFileForWriting(true);
				writeCurrentStepData(adf);
			}
		}
		finally{
			//On ferme le fichier
			releaseFile();
			if(adf != null && !keepFileOpened) adf.close();
			lock.writeLock().unlock();
		}
	}
	
	
	/**
	 * Enregistre les valeurs du step en cours dans le fichier
	 * @throws IOException 
	 */
	private void writePoint(RandomAccessFile adf) throws IOException{
		if(stepNb == 0) return;
		
		AbsCounterArchivePoint point = (AbsCounterArchivePoint)currentStepPoint();
		if(point==null){
			return;
		}
		
		long len = adf.length();
		
		
		if(len<HEADER1_LEN + AbsCounterArchive.CURRENT_STEP_DATA_LENGTH){
			/*
			 * Premier enregistrement de l'archive
			 */
			adf.seek(8);	//TODO ne pas coder en dur si possible: correspond à l'enregistrement de step+type
			//Timestamp de début de l'archive
			adf.writeLong(stepTimestamp);
			for(int i=0;i<currentStepDataLength();i++) adf.write((byte)0);
			len = adf.length();
		}
		else{
			/*
			 * Enregistrement placé à la fin
			 */
			adf.seek(len);
		}
		
		if(lastTimestamp!=null && stepTimestamp == lastTimestamp){
			/*
			 * On écrase la dernière valeur qui correspond au meme step. (C'est autorise)
			 * TODO : PAS SUR, A VERIFIER A CAUSE DU DIFF
			 */
			logger.fine("override last value");
			adf.seek(adf.getFilePointer() - ENREG_LEN);
		}
		else if(lastTimestamp!=null && stepTimestamp > lastTimestamp+step){
			//On remplit eventuellement les steps sans valeur
			int stepsToSkip = (int)(stepTimestamp - lastTimestamp - step)/step;
			logger.fine("skip "+stepsToSkip+" steps");
			//Il y a des steps vides
			for(int i=0;i<stepsToSkip;i++){
				adf.writeByte(0x00);
				for(int j=0;j<ENREG_LEN-1;j++){
					adf.writeByte(0);
				}
			}
		}
		
		//On ecrit la nouvelle valeur
		logger.fine("flush step "+sdf.format(new Date(point.timestamp*1000))+": value="+point.value);
		int flags = 0x01;		//set first flag bit
		if(point.overflow) flags = flags & 0x02;	//set second flag bit
		adf.writeByte(flags);
		if(point.value==null){
			logger.fine("null");
		}
		adf.writeFloat(point.value);
		adf.writeFloat(point.diff);
		
		//Mise a jour du timestamp de dernier enregistrement
		this.lastTimestamp = stepTimestamp;
		
		//Conservation du dernier point
		this.previousPoint = point;
	}
	
	/**
	 * Lit le point dans le fichier, a la position du curseur
	 * @throws IOException 
	 */
	protected ArchivePoint readPoint(RandomAccessFile raf) throws IOException{
		AbsCounterArchivePoint p = new AbsCounterArchivePoint();
		byte flags = raf.readByte();
		boolean definedFlag = (flags & 0x01) > 0;
		if(definedFlag){
			p.value = raf.readFloat();
			p.diff = raf.readFloat();
			p.overflow = (flags & 0x02) > 0;
		}
		return p;
	}
	
	@Override
	protected ArchivePoint newEmptyPoint(Long timestamp){
		AbsCounterArchivePoint p = new AbsCounterArchivePoint();
		p.timestamp = timestamp;
		return p;
	}
}
