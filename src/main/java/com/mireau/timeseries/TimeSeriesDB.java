package com.mireau.timeseries;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Classe principale de gestion de la base de données de series de données
 */
public class TimeSeriesDB {

	File dbDirectory;
	ConcurrentMap<String, TimeSerie> timeseries;
	
	public TimeSeriesDB(File dbDirectory) {
		this.dbDirectory = dbDirectory;
		init();
	}

	public void init(){
		timeseries = new ConcurrentHashMap<>();
		
		/*
		 * Lecture du répertoire 
		 */
		final Pattern rawFilenamePattern = Pattern.compile("ts_(.*)\\.rts", Pattern.CASE_INSENSITIVE);
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
			ts = createTimeSerie(name);
			timeseries.put(name,ts);
		}
		return ts;
	}
	
	public TimeSerie createTimeSerie(String name) throws IOException, TimeSerieException{
		TimeSerie ts = new TimeSerie(name, this.dbDirectory);
		return ts;
	}
	
	public Collection<TimeSerie> getTimeSeries(){
		return timeseries.values();
	}

	public ConcurrentMap<String, TimeSerie> getTimeseries() {
		return timeseries;
	}
}
