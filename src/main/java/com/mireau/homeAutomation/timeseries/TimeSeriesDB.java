package com.mireau.homeAutomation.timeseries;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import com.mireau.homeAutomation.timeseries.ArchiveDataSerie.Type;
import com.mireau.homeAutomation.timeseries.RawDataSerie.Entry;

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
	
	/** Répertoire de stockage des fichiers de données */
	private File directory;
	
	
	
	public TimeSeriesDB(String id, String dir) throws IOException, ArchiveInitException{
		this.id = id;
		this.directory = new File(dir);
		
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
			archives.add(ArchiveDataSerie.getArchive(file,id));
		}
	}
	
	
	public void close() throws IOException{
		rawDS.close();
		for (ArchiveDataSerie archive : archives) {
			if(archive !=null) archive.close();
		}
	}
	
	public boolean isClosed(){
		return rawDS==null;
	}
	
	public ArchiveDataSerie getArchive(int step, Type type){
		for (ArchiveDataSerie archive : archives) {
			if(archive.step==step && archive.type()==type) return archive;
		}
		return null;
	}
	
	
	/**
	 * Ajoute une valeur à la fin de la série.
	 * Le timestamp doit être postérieur à tout autre point de la série
	 */
	public void post(long timestamp, float value) throws IOException{
		rawDS.post(timestamp,value);
		for (ArchiveDataSerie archive : archives) {
			archive.post(timestamp,value);
		}
	}
	public void post(long timestamp, double value) throws IOException{
		post(timestamp,(float)value);
	}

	/**
	 * Construit une archive à partir des données brutes 
	 * @param step
	 * @throws ArchiveInitException 
	 * @throws IOException 
	 */
	public void buildArchive(int step, Type type) throws IOException, ArchiveInitException{
		ArchiveDataSerie archive = getArchive(step,type);
		if(archive==null){
			//L'archive n'existe pas
			String filename = "ts_"+id+"_"+step+".ats";
			File file = new File(directory,filename);
			archive = ArchiveDataSerie.getArchive(file, this.id, step, type);
		}
		
		Iterator<Entry> iter = rawDS.iterator(null,null);
		archive.build(iter);
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
