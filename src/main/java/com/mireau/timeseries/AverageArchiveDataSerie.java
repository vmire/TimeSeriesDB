package com.mireau.timeseries;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Date;

public class AverageArchiveDataSerie extends ArchiveDataSerie {

	static int ENREG_LEN = 13;
	static int CURRENT_STEP_DATA_LENGTH = 28;
	
	
	/** somme des valeurs pour le step en cours (pour le type AVERAGE uniquement)*/
	double stepSum;
	/** nombre de valeurs pour le step en cours (pour le type AVERAGE uniquement)*/
	int stepNb;
	/** dernière valeur sur le step en cours*/
	Float stepLast;
	/** valeur mini sur le step en cours*/
	Float stepMin;
	/** valeur maxi sur le step en cours*/
	Float stepMax;
	/** timestamp du debut de step en cours */
	Long stepTimestamp;
	
	
	
	public AverageArchiveDataSerie(File file, String id, Integer step) throws IOException, ArchiveInitException {
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
		logger.fine("write current step vars : "+sdf.format(new Date((long)stepTimestamp*1000))+" nb="+stepNb+" sum="+stepSum);
		raf.seek(CUR_STEP_RECORD_POS);
		raf.writeLong(stepTimestamp);
		raf.writeDouble(stepSum);
		raf.writeInt(stepNb);
		raf.writeFloat(stepMin==null ? 0 : stepMin);
		raf.writeFloat(stepMax==null ? 0 : stepMax);
	}
	
	/**
	 * reinitialisation des variables sur le step en cours
	 */
	protected void resetCurrentStepData(){
		stepTimestamp = null; 
		stepSum = 0;
		stepNb = 0;
		stepMin = null;
		stepMax = null;
	} 
	/**
	 * Lecture des données sur le step en cours
	 */
	protected void readCurrentStepData(RandomAccessFile adf) throws IOException{
		adf.seek(CUR_STEP_RECORD_POS);
		
		stepTimestamp = adf.readLong();
		stepSum = adf.readDouble();
		stepNb = adf.readInt();
		stepMin = adf.readFloat();
		stepMax = adf.readFloat();
		
		if(stepNb==0){
			stepMin = null;
			stepMax = null;
		}
		logger.fine("step en cours: sum="+stepSum+" nb="+stepNb+" min="+stepMin+" max="+stepMax+" timestamp="+stepTimestamp);
	}
	
	/**
	 * Longueur des données du step en cours écrites dans le fichier archive
	 */
	protected int currentStepDataLength(){
		return CURRENT_STEP_DATA_LENGTH;
	}
	
	/**
	 * Construit l'objet ArchivePoint correspondant aux valeurs enregistrées sur le step en cours
	 */
	protected ArchivePoint currentStepPoint(){
		if(this.stepNb==0) return null;
		AverageArchivePoint point = new AverageArchivePoint();
		point.value = (float)(this.stepSum / this.stepNb);
		point.min = this.stepMin;
		point.max = this.stepMax;
		point.timestamp = this.stepTimestamp;
		return point;
	}
	
	/**
	 * Enregistrement d'un point
	 * @param timestamp
	 * @param value
	 * @throws IOException
	 * @throws ArchiveInitException 
	 */
	@Override
	public void post(long timestamp, float value) throws IOException, ArchiveInitException{
		post(timestamp,value,null);
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
		
		if(stepTimestamp!=null && stepTimestamp>0 && timestamp < stepTimestamp){
			logger.warning("Nouvelle valeur anterieure au step en cours (cur step:"+sdf.format(new Date(stepTimestamp*1000))+" new value:"+sdf.format(new Date(timestamp*1000))+")");
		}
		
		if(stepTimestamp==null || stepTimestamp==0){
			//Calcul du timestamp d'origine de l'archive : arrondi au step immédiatement inférieur
			stepTimestamp = getTimestampOrigine(timestamp);
			logger.fine("wite start timestamp");
			//ecriture du timestamp dans l'en-tête
			if(adf==null) adf = openFile("rw");
			adf.seek(8);	//on se positionne sur le champ timestamp de début
			adf.writeLong(stepTimestamp);
		}
		else if(timestamp >= stepTimestamp+step){
			/*
			 * Changement de step -> écriture
			 */	
			if(adf==null) adf = openFile("rw");
			this.writePoint(adf);
		
			this.stepSum = 0;
			this.stepNb = 0;
			this.stepMin = null;
			this.stepMax = null;
		
			long nbSteps = (timestamp - lastTimestamp)/step;
			this.stepTimestamp = this.lastTimestamp + nbSteps*step;
			logger.fine("nouveau step : "+sdf.format(new Date(stepTimestamp*1000)));
		}
		
		this.stepLast = value;
	
		this.stepSum += value;
		this.stepNb++;
		if(this.stepMin==null || value < this.stepMin) this.stepMin = value;
		if(this.stepMax==null || value > this.stepMax) this.stepMax = value;
		
		logger.fine("add to current step : "+stepLast+" nb="+stepNb+" sum="+stepSum);
		
		if(this.writeStartegy == WriteStrategy.ALL_POINTS){
			//On écrit systématiquement le données du step en cours
			//Plus sûr, mais moins performant
			if(adf==null) adf = openFile("rw");
			writeCurrentStepData(adf);
		}
		
		//On ferme le fichier
		if(!keepFileOpened) adf.close();
	}
	
	
	/**
	 * Enregistre les valeurs du step en cours dans le fichier
	 * @throws IOException 
	 */
	private void writePoint(RandomAccessFile adf) throws IOException{
		if(stepNb == 0) return;
		
		AverageArchivePoint point = (AverageArchivePoint)currentStepPoint();
		
		long len = adf.length();
		
		
		if(len<HEADER1_LEN + AverageArchiveDataSerie.CURRENT_STEP_DATA_LENGTH){
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
				adf.writeBoolean(false);
				for(int j=0;j<ENREG_LEN-1;j++){
					adf.writeByte(0);
				}
			}
		}
		
		//On ecrit la nouvelle valeur
		logger.fine("flush step "+sdf.format(new Date(point.timestamp*1000))+": value="+point.value);
		adf.writeBoolean(true);
		adf.writeFloat(point.value);
		adf.writeFloat(point.min);
		adf.writeFloat(point.max);
		
		//Mise a jour du timestamp de dernier enregistrement
		this.lastTimestamp = stepTimestamp;
	}
	
	/**
	 * Lit le point dans le fichier, a la position du curseur
	 * @throws IOException 
	 */
	protected ArchivePoint readPoint(RandomAccessFile raf) throws IOException{
		AverageArchivePoint p = new AverageArchivePoint();
		if(raf.readBoolean()){
			p.value = raf.readFloat();
			p.min = raf.readFloat();
			p.max = raf.readFloat();
		}
		return p;
	}
	
	@Override
	protected ArchivePoint newEmptyPoint(Long timestamp){
		AverageArchivePoint p = new AverageArchivePoint();
		p.timestamp = timestamp;
		return p;
	}
}
