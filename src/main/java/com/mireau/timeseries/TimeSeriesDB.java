package com.mireau.timeseries;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Classe principale de gestion de la base de données de series de données
 */
public class TimeSeriesDB {

	public static String RAW_TIMESERIE_FILE_EXT = "rts";
	public static String ARCHIVE_TIMESERIE_FILE_EXT = "ats";
	public static String META_TIMESERIE_FILE_EXT = "mts";
	public static String NODE_FILE_EXT = "node";
	public static String FILENAME_PREFIX = "ts_";
	
	File dbDirectory;
	
	/** table des timeseries */
	ConcurrentMap<String, TimeSerie> timeseries;
	
	/** noeuds */
	ConcurrentMap<String, Node> nodes;
	
	public TimeSeriesDB(File dbDirectory) {
		this.dbDirectory = dbDirectory;
		init();
	}

	public void init(){
		timeseries = new ConcurrentHashMap<String, TimeSerie>();
		nodes = new ConcurrentHashMap<String, Node>();
		
		/*
		 * Recherche des fichiers de noeuds
		 */
		final Pattern nodeFilenamePattern = Pattern.compile(FILENAME_PREFIX+"(.*)\\."+NODE_FILE_EXT, Pattern.CASE_INSENSITIVE);
		dbDirectory.list(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String filename) {
				Matcher m = nodeFilenamePattern.matcher(filename);
				
				if(m.matches()){
					String id = m.group(1);
					System.out.println("node:"+id+" "+dir.getAbsolutePath());
					try{
						Node n = new Node(id,dir);
						nodes.put(id, n);
					} catch (IOException | TimeSerieException e) {
						e.printStackTrace();
					}
					return true;
				}
				else{
					return false;
				}
			}
		});
		
		/*
		 * Recherche des fichiers timeseries (raw) 
		 */
		final Pattern rawFilenamePattern = Pattern.compile(FILENAME_PREFIX+"(.*)\\."+RAW_TIMESERIE_FILE_EXT, Pattern.CASE_INSENSITIVE);
		dbDirectory.list(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String filename) {
				Matcher m = rawFilenamePattern.matcher(filename);
				if(m.matches()){
					System.out.println("db:"+m.group(1)+" "+dir.getAbsolutePath());
					String id = m.group(1);
					try {
						TimeSerie ts = new TimeSerie(id,dir);
						timeseries.put(id, ts);
					} catch (IOException | TimeSerieException e) {
						e.printStackTrace();
					}
					return true;
				}
				else{
					return false;
				}
			}
		});
	}
	
	public TimeSerie getTimeSerie(String name){
		TimeSerie ts = timeseries.get(name);
		return ts;
	}
	
	public TimeSerie getTimeSerie(String name, boolean createIfNotExists) throws IOException, TimeSerieException{
		TimeSerie ts = getTimeSerie(name);
		if(ts==null && createIfNotExists){
			//Création de la nouvelle TimeSerie
			ts = new TimeSerie(name, this.dbDirectory);
			timeseries.put(name,ts);
		}
		return ts;
	}
	
	
	/**
	 * Supprime la timeserie
	 * @throws TimeSerieException 
	 */
	public void deleteTimeSerie(String name) throws IOException, TimeSerieException{
		TimeSerie ts = getTimeSerie(name,false);
		
		//Suppression de toutes les archives
		Iterator<Archive> iter = ts.archives.iterator();
		while(iter.hasNext()) {
			Archive a = iter.next();
			a.archiveFile.delete();
			iter.remove();
		}
		
		//Supression des metadatas
		ts.meta.metadataFile.delete();
		
		//Suppression de la timeserie
		ts.rawDS.rawFile.delete();
		
		this.timeseries.remove(name);
	}
	
	public Collection<TimeSerie> getTimeSeries(){
		return timeseries.values();
	}

	public ConcurrentMap<String, TimeSerie> getTimeseries() {
		return timeseries;
	}
	
	public Node getNode(String name){
		Node node = nodes.get(name);
		return node;
	}
	
	public Node getNode(String name, boolean createIfNotExists) throws IOException, TimeSerieException{
		Node node = getNode(name);
		if(node==null && createIfNotExists){
			//Création du nouveau noeud
			node = new Node(name, this.dbDirectory);
			node.record();
			nodes.put(name,node);
		}
		return node;
	}
}
