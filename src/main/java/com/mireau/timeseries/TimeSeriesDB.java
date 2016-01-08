package com.mireau.timeseries;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import com.mireau.timeseries.ArchiveDataSerie.Type;
import com.mireau.timeseries.RawDataSerie.Entry;

/**
 * Base de données de séries de données chronologiques à intervale variable
 * 
 * 
 */
public class TimeSeriesDB {
	
	Logger logger = Logger.getLogger(TimeSeriesDB.class.getName());

	/** nom (identifiant) de la série */
	private String id;
	
	/** RawDataSerie */
	RawDataSerie rawDS;
	
	/** Liste des archives associées */
	List<ArchiveDataSerie> archives;
	
	/** R�pertoire de stockage des fichiers de donn�es */
	private File directory;
	
	
	
	public TimeSeriesDB(String id, String dir) throws IOException, ArchiveInitException{
		this(id,new File(dir));
	}
	
	public TimeSeriesDB(String id, File dir) throws IOException, ArchiveInitException{
		this.id = id;
		this.directory = dir;
		
		/*
		 * Raw
		 */
		rawDS = new RawDataSerie(directory,id);
		
		archives = new ArrayList<ArchiveDataSerie>();
		
		//On parcours le répertoire
		File[] files = directory.listFiles();
		Pattern filenamePattern = Pattern.compile("ts_"+id+"_[0-9]+[_a-zA-Z]*\\.ats", Pattern.CASE_INSENSITIVE);
		for (File file : files) {
			if(!filenamePattern.matcher(file.getName()).matches()) continue;
			ArchiveDataSerie archive = ArchiveDataSerie.getArchive(file,id);
			if(archive==null){
				logger.warning("skip archive file : null");
				continue;
			}
			archives.add(archive);
		}
	}
	
	
	public void close() throws IOException, ArchiveInitException{
		rawDS.close();
		for (ArchiveDataSerie archive : archives) {
			if(archive !=null) archive.close();
		}
	}
	
	public boolean isClosed(){
		return rawDS==null;
	}
	
	public ArchiveDataSerie getArchive(int step, Type type){
		for(ArchiveDataSerie a : archives) {
			if(a.step==step && a.type()==type) return a;
		}
		return null;
	}
	
	/**
	 * Crée une archive (le fichier n'existe pas déjà)
	 * @param step
	 * @param type
	 * @return
	 * @throws IOException
	 * @throws ArchiveInitException
	 */
	public ArchiveDataSerie createArchive(int step, Type type) throws IOException, ArchiveInitException{
		ArchiveDataSerie archive = getArchive(step,type);
		if(archive!=null) throw new ArchiveInitException("l'archive existe deja");
		
		String filename = "ts_"+id+"_"+step+".ats";
		File file = new File(directory,filename);
		if(file.exists()) throw new ArchiveInitException("le fichier "+file.getAbsolutePath()+" existe deja");
		
		ArchiveDataSerie.newArchiveFile(step, type, file);;
		archive = ArchiveDataSerie.getArchive(file, this.id, step, type);
		
		archives.add(archive);
		return archive;
	}
	
	/**
	 * Construit une archive a partir des données brutes 
	 * @param step
	 * @return ArchiveDataSerie
	 * @throws ArchiveInitException 
	 * @throws IOException 
	 */
	public void buildArchive(int step, Type type) throws IOException, ArchiveInitException{
		ArchiveDataSerie archive = getArchive(step,type);
		if(archive==null) archive = createArchive(step, type);
		
		if(rawDS!=null){
			Iterator<Entry> iter = rawDS.iterator(null,null);
			archive.build(iter);
		}
	}
	
	/**
	 * Ajoute une valeur à la fin de la serie.
	 * Le timestamp doit etre posterieur a tout autre point de la serie
	 * @throws ArchiveInitException 
	 */
	public void post(Date date, float value) throws IOException, ArchiveInitException{
		this.post(date.getTime()/1000,value);
	}
	public void post(Date date, double value) throws IOException, ArchiveInitException{
		this.post(date.getTime()/1000,value);
	}
	public void post(long timestamp, float value) throws IOException, ArchiveInitException{
		rawDS.post(timestamp,value);
		for (ArchiveDataSerie archive : archives) {
			archive.post(timestamp,value);
		}
	}
	public void post(long timestamp, double value) throws IOException, ArchiveInitException{
		post(timestamp,(float)value);
	}
	/**
	 * Récupère la dernière valeur brute transmise
	 * @return
	 * @throws IOException 
	 */
	public Entry getLast() throws IOException{
		return rawDS.getLast();
	}
	
}
