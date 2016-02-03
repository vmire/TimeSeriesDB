package com.mireau.timeseries;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import com.mireau.timeseries.RawData.Entry;

/**
 * Base de données de séries de données chronologiques à intervale variable
 * 
 * 
 */
public class TimeSerie {
	
	Logger logger = Logger.getLogger(TimeSerie.class.getName());

	/** nom (identifiant) de la série */
	private String id;
	
	
	/** Meta données */
	Meta meta;
	
	public Meta getMeta() {
		return meta;
	}

	/** RawDataSerie */
	RawData rawDS;
	
	/** Liste des archives associées */
	List<Archive> archives;
	
	/** Répertoire de stockage des fichiers de données */
	private File directory;
	
	
	
	protected TimeSerie(String id, String dir) throws IOException, ArchiveInitException{
		this(id,new File(dir));
	}
	
	protected TimeSerie(String id, File dir) throws IOException, ArchiveInitException{
		this.id = id;
		this.directory = dir;
		
		/*
		 * Méta-données
		 */
		File metadataFile = new File(dir,"ts_"+id+".mts");
		meta = new Meta(metadataFile);
		meta.readMetadata();
		
		/*
		 * Raw
		 */
		File rawFile = new File(dir,"ts_"+id+".rts");
		rawDS = new RawData(rawFile);
		
		/*
		 * Archives
		 */
		archives = new ArrayList<Archive>();
		
		//On parcours le répertoire
		File[] files = directory.listFiles();
		Pattern archiveFilenamePattern = Pattern.compile("ts_"+id+"_[0-9]+[_a-zA-Z]*\\.ats", Pattern.CASE_INSENSITIVE);
		for (File file : files) {
			/*
			 * Archive
			 */
			if(archiveFilenamePattern.matcher(file.getName()).matches()){
				Archive archive = Archive.getArchive(file,id);
				if(archive==null){
					logger.warning("skip archive file : null");
					continue;
				}
				archives.add(archive);
			}
		}
	}
	
	
	
	public void close() throws IOException, ArchiveInitException{
		rawDS.close();
		for (Archive archive : archives) {
			if(archive !=null) archive.close();
		}
	}
	
	public boolean isClosed(){
		return rawDS==null;
	}
	
	public Archive getArchive(int step){
		for(Archive a : archives) {
			if(a.step==step) return a;
		}
		return null;
	}
	
	/**
	 * Crée une archive (le fichier n'existe pas déjà)
	 * @param step
	 * @return
	 * @throws IOException
	 * @throws ArchiveInitException
	 */
	public Archive createArchive(int step) throws IOException, ArchiveInitException{
		Archive archive = getArchive(step);
		if(archive!=null) throw new ArchiveInitException("l'archive existe deja");
		
		String filename = "ts_"+id+"_"+step+".ats";
		File file = new File(directory,filename);
		if(file.exists()) throw new ArchiveInitException("le fichier "+file.getAbsolutePath()+" existe deja");
		if(this.getMeta().getType()==null)  throw new ArchiveInitException("type is not initialized");
		
		Archive.newArchiveFile(step, this.getMeta().getType(), file);
		archive = Archive.getArchive(file, this.id);
		buildArchive(archive);
		
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
	public void buildArchive(Archive archive) throws IOException, ArchiveInitException{
		if(rawDS!=null){
			Iterator<Entry> iter = rawDS.iterator(null,null);
			archive.build(iter);
		}
	}
	
	/**
	 * Supprime une archive
	 * @param step
	 * @return
	 * @throws IOException
	 * @throws ArchiveInitException
	 */
	public void removeArchive(int step) throws IOException, ArchiveInitException{
		Iterator<Archive> iter = archives.iterator();
		while(iter.hasNext()) {
			Archive a = iter.next();
			if(a.step==step){
				a.archiveFile.delete();
				iter.remove();
				break;
			}
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
		for (Archive archive : archives) {
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
	
	public List<ArchivePoint> select(int step, Long start, int period) throws ArchiveInitException, IOException, InterruptedException{
		Long end = null;
		if(start==null){
			end = new Date().getTime()/1000;
			start = end-period;
		}
		else{
			end = start+period;
		}
		Archive archive = this.getArchive(step);
		if(archive==null)
			throw new ArchiveInitException("Erreur : aucune archive avec step="+step);
		
		List<ArchivePoint> list = archive.getPoints(start,end);
		return list;
	}
	
	public List<ArchivePoint> select(int step, Long start, String periodStr) throws ArchiveInitException, IOException, InterruptedException{
		char unit = periodStr.charAt(periodStr.length()-1);
		int multipl = 1;
		if(unit=='h' || unit=='d' || unit=='w' || unit=='m' || unit=='y'){
			periodStr = periodStr.substring(0,periodStr.length()-1);
			switch(unit){
				case 'h': multipl = 3600; break;
				case 'd': multipl = 86400; break;
				case 'w': multipl = 604800; break;
				case 'm': multipl = 2678400; break;
				case 'y': multipl = 31536000; break;
			}
		}
		int period = Integer.parseInt(periodStr) * multipl;
		
		return select(step,start,period);
	}
	
	
	public void exportCSV(final List<ArchivePoint> points, PrintStream out, DateFormat dateFormat, NumberFormat numberFormat) throws ArchiveInitException, IOException{
		for (ArchivePoint point : points) {
			out.println(point.csv(dateFormat,numberFormat));
		}
	}
	
	public void exportJSON(final List<ArchivePoint> points, PrintStream out, DateFormat dateFormat, NumberFormat numberFormat) throws ArchiveInitException, IOException{
		out.print("[");
		boolean first = true;
		for (ArchivePoint point : points) {
			if(!first) out.print(",");
			else first = false;
			out.println(point.json(dateFormat,numberFormat));
		}
		out.println("]");
	}

	public String getId() {
		return id;
	}

	public List<Archive> getArchives() {
		return archives;
	}

	public RawData getRawDS() {
		return rawDS;
	}
	
	
}
