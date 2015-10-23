package com.mireau.homeAutomation.timeseries;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Date;

public class AverageArchiveDataSerie extends ArchiveDataSerie {

	static int ENREG_LEN = 13;
	static int CURRENT_STEP_DATA_LENGTH = 44;
	
	
	/** somme des valeurs pour le step en cours (pour le type AVERAGE uniquement)*/
	double stepSum;
	/** nombre de valeurs pour le step en cours (pour le type AVERAGE uniquement)*/
	int stepNb;
	/** dernière valeur sur le step en cours*/
	Float stepLast = null;
	/** valeur mini sur le step en cours*/
	Float stepMin = null;
	/** valeur maxi sur le step en cours*/
	Float stepMax = null;
	/** timestamp du début de step en cours */
	Long stepTimestamp;
	
	
	
	public AverageArchiveDataSerie(File file, String id, Integer step) throws IOException, ArchiveInitException {
		super(file, id, step);
		
		stepLast = null;
		stepMax = null;
		stepMin = null;
		stepNb = 0;
		stepSum = 0;
		stepTimestamp = (long)0;
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
	 */
	protected synchronized void writeCurrentStepData(RandomAccessFile raf) throws IOException{
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
		logger.fine("step en cours: sum="+stepSum+" nb="+stepNb+" min="+stepMin+" max="+stepMax);
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
	 */
	public void post(long timestamp, float value) throws IOException{
		
		if(lastTimestamp==null || lastTimestamp == 0){
			//Il n'y a encore rien dans cette archive
			lastTimestamp = getTimestampOrigine(timestamp);
			stepTimestamp = lastTimestamp;
			
			RandomAccessFile adf = openFile("rw");
			
			adf.seek(8);	//TODO à ne pas coder en dur
			adf.writeLong(lastTimestamp);
			
			releaseFile(adf);
		}
		
		if(stepTimestamp!=null && timestamp < stepTimestamp){
			logger.warning("Nouvelle valeur antérieure au step en cours (cur step:"+sdf.format(new Date(stepTimestamp*1000))+" new value:"+sdf.format(new Date(timestamp*1000))+")");
		}
		
		//Ecriture du step en cours si nécessaire
		if(stepTimestamp!=null && timestamp >= stepTimestamp+step){
			/*
			 * Changement de step
			 */
			this.writePoint();
			
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
		
		logger.fine("last="+stepLast+" nb="+stepNb+" sum="+stepSum);
		
	}
	
	
	/**
	 * Enregistre les valeurs du step en cours dans le fichier
	 * @throws IOException 
	 */
	protected void writePoint() throws IOException{
		if(stepNb == 0) return;
		
		AverageArchivePoint point = (AverageArchivePoint)currentStepPoint();
		
		RandomAccessFile adf = openFile("rw");
		
		long len = adf.length();
		adf.seek(len);
		
		if(stepTimestamp == lastTimestamp){
			/*
			 * On écrase la dernière valeur qui correspond au même step. (C'est autorisé)
			 */
			logger.fine("override last value");
			adf.seek(adf.getFilePointer() - ENREG_LEN);
		}
		else if(stepTimestamp > lastTimestamp+step){
			//On remplit éventuellement les steps sans valeur
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
		
		//On écrit la nouvelle valeur
		logger.fine("flush step "+sdf.format(new Date(point.timestamp*1000))+": value="+point.value);
		adf.writeBoolean(true);
		adf.writeFloat(point.value);
		adf.writeFloat(point.min);
		adf.writeFloat(point.max);
		
		//Mise à jour du timestamp de dernier enregistrement
		this.lastTimestamp = stepTimestamp;
		
		//On relache le fichier
		releaseFile(adf);
	}
	
	/**
	 * Lit le point dans le fichier, à la position du curseur
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
	
	/**
	 * Classe qui représente un point de l'archive
	 */
	public class AverageArchivePoint extends ArchivePoint{
		Float min = null;
		Float max = null;
		public Float getMin(){ return min; }
		public Float getMax(){ return max; }
	}
}
