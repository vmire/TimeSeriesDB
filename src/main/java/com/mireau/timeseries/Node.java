package com.mireau.timeseries;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

public class Node {
	
	Logger logger = Logger.getLogger(Node.class.getName());
	
	/**
	 * Id du noeud
	 */
	String id;
	
	/**
	 * Répertoire
	 */
	File directory;
	
	/**
	 * Libellé descriptif du noeud
	 */
	public String libelle;
	
	/**
	 * Fichier
	 */
	File nodeFile;
	
	protected Node(String id, File dir) throws IOException, TimeSerieException{
		this.id = id;
		this.directory = dir;
		
		//Vérification du nom
		String regex = "[a-zA-Z0-9._-]+";
		if(!id.matches(regex)){
			throw new TimeSerieException("Node id contains invalid caracters (valids are: "+regex+") :"+id);
		}
		
		/*
		 * Fichier
		 */
		nodeFile = new File(dir,TimeSeriesDB.FILENAME_PREFIX+id+"."+TimeSeriesDB.NODE_FILE_EXT);	
	}
	
	/**
	 * Timeseries
	 */
	ConcurrentMap<String, TimeSerie> timeseries;
	
	/**
	 * Création du fichier qui représente le noeud
	 * @throws IOException
	 */
	public void record() throws IOException{
		nodeFile.createNewFile();
	}
	
	/**
	 * Suppression du fichier qui représente le noeud
	 * @throws IOException
	 */
	public void delete() throws IOException{
		nodeFile.delete();
	}
}
