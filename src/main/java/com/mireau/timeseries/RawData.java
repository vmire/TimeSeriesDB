package com.mireau.timeseries;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

/** 
 * Enregistrement des données brutes.
 * Peut être scindé en plusieurs fichiers (TODO)
 * Format:
 *   En tête:
 *     aucun
 *   Chaque enregistrement : 8 bytes 
 *     timestamp (secondes) : int  (4 bytes/32bits/unsigned -> 31bits) : 68 ans (2038)
 *     valeur (signed)      : float(4 bytes)
 *     pas de caractère de séparation
 */
public class RawData {
	
	static Logger logger = Logger.getLogger(RawData.class.getName());
	static int DATA_LEN = 8;
	static SimpleDateFormat sdf = new SimpleDateFormat("YYYY/MM/dd HH:mm:ss");
	
	String id;
	File rawFile;
	
	/** dernier enregistrement */
	protected Entry last;
	
	protected RawData(File f){
		this.rawFile = f;
	}
	
	public File getFile() throws IOException{
		rawFile.createNewFile();		//Vérifie si le fichier n'existe pas deja
		return rawFile;
	}
	
	protected void close(){
		
	}
	
	/**
	 * Fourni la dernière valeur
	 * @return
	 * @throws IOException 
	 */
	public Entry getLast() throws IOException{
		return getLast(null);
	}
	
	/**
	 * Fourni la dernière valeur
	 * Le curseur est positionné en fin de dernière valeur (pret à écrire si ouvert en rw)
	 * @return
	 * @throws IOException 
	 */
	private Entry getLast(RandomAccessFile rdf) throws IOException{
		if(this.last==null){
			File file = getFile();
			long len = file.length();
			if(len>=DATA_LEN){
				//Il y a des données dans le fichier : lecture
				boolean closeFileFlag = false;
				if(rdf==null){
					rdf = new RandomAccessFile(file,"r");
					closeFileFlag = true;
				}
				long pos = len - DATA_LEN;
				long mod = pos % DATA_LEN;
				if(mod != 0){
					logger.warning("Taille de fichier incoherence ("+len/DATA_LEN+"x"+DATA_LEN+", reste "+mod+"). retour a "+(len-mod));
					pos -= mod;
				}
				rdf.seek(pos);
				
				last = new Entry();
				last.timestamp = (long)rdf.readInt();
				last.value = rdf.readFloat();
				logger.fine("dernier enregistrement: "+last);
				
				if(closeFileFlag) rdf.close();
			}
			
		}
		
		return this.last;
	}
	
	
	public void post(long timestamp, float value) throws IOException{
		File file = getFile();
		RandomAccessFile rdf = null;
		
		try{
			rdf = new RandomAccessFile(file,"rw");
			
			//On vérifie que la longueur est cohérente
			long len = rdf.length();
			if(len>0){
				int mod = (int)(len % DATA_LEN);
				if(mod != 0){
					logger.warning("Taille de fichier incoherence ("+len/DATA_LEN+"x"+DATA_LEN+", reste "+mod+"). retour a "+(len-mod));
					len  = len - mod;
				}
				
				if(timestamp < getLast(rdf).timestamp){
					logger.warning("la nouvelle valeur anterieure a la derniere (prev:"+sdf.format(new Date((long)last.timestamp*1000))+" new:"+sdf.format(new Date((long)timestamp*1000))+")");
				}
				
				//Positionnement en fin de fichier
				rdf.seek(len);
			}
			
			//On écrit l'enregistrement
			if(timestamp>Integer.MAX_VALUE){
				logger.warning("timestamp tronqué : "+timestamp+"/"+(int)timestamp);
			}
			
			if(last==null) last = new Entry();
			last.timestamp = (int)timestamp;
			last.value = value;
			
			logger.fine("write: "+value+"("+sdf.format(new Date(timestamp*1000))+")");
			rdf.writeInt((int)last.timestamp);
			rdf.writeFloat(last.value);
		}
		finally {
			if(rdf != null) rdf.close();
		}
		
	}
	
	public List<Entry> getLastPoints(int nb) throws IOException{
		File file = getFile();
		RandomAccessFile raf = null;
		List<Entry> result = null;
		
		try{
			raf = new RandomAccessFile(file, "r");
			long pos = raf.length() - ( nb * DATA_LEN );
			if(pos<0){
				nb = nb+(int)(pos/DATA_LEN);
				pos = 0;
			}
			raf.seek(pos);
			
			result = new ArrayList<Entry>(nb);
			Entry next = null;
			for(int i=0;i<nb;i++){
				next = new Entry();
				next.timestamp = (long)raf.readInt();
				next.value = raf.readFloat();
				result.add(next);
			}
		}
		finally{
			if(raf!=null) raf.close();
		}
		return result;
	}
	
	/**
	 * Parcours la liste des enregistrement sur une période
	 * 
	 * @param beginTimestamp
	 * @param endTimestamp
	 * @return
	 * @throws IOException 
	 */
	public Iterator<Entry> iterator(Long beginTimestamp, Long endTimestamp) throws IOException{
		return new RDSIterator(getFile(),beginTimestamp,endTimestamp);
	}
	
	/**
	 * Représente une entrée dans la série
	 */
	public class Entry{
		long timestamp;
		float value;
		public String toString(){
			return RawData.sdf.format(new Date(timestamp*1000))+":"+value;
		}
		public long getTimestamp() {
			return timestamp;
		}
		public float getValue() {
			return value;
		}
	}
	
	/**
	 * Classe pour la lecture séquentielle de la série
	 */
	public class RDSIterator implements Iterator<Entry>{

		RandomAccessFile raf;
		Long begin = null;
		Long end = null;
		Long length = null;
		
		Entry next = null;
		boolean flagNext;	//Indique si la valeur suivante a été extraite (avant de la retourner)
		
		/**
		 * 
		 * @param file
		 * @param begin
		 * @param end
		 * @throws FileNotFoundException
		 */
		protected RDSIterator(File file, Long beginTimestamp, Long endTimestamp) throws FileNotFoundException{
			this.raf = new RandomAccessFile(file, "r");
			this.begin = beginTimestamp;
			this.end = endTimestamp;
			
			if(begin!=null && begin>0){
				//Il y a un timestamp de début spécifié
				//Recherche du point de départ dans le fichier
				try {
					long p1 = 0;			//position limite de gauche
					long p2 = raf.length();	//position limite de droite
					while(true){
						if(p2-p1 <= DATA_LEN){
							//On ne peut pas plus raffiner : le timestamp ne tombe pas exactement
							//On prend le timestamp supérieur
							raf.seek(p2);
							break;
						}
						long middle = (p2-p1)/2;
						long mpos = (middle/DATA_LEN)*DATA_LEN;
						raf.seek(mpos);
						int t = raf.readInt();
						if(t==begin){
							//On a trouvé le timestamp exact
							//On ramène le curseur sur cette position
							raf.seek(mpos);
							break;
						}
						else if(t>begin){
							//c'est à gauche
							p2 = mpos;
						}
						else{
							//c'est à droite
							p1 = mpos;
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		
		public boolean hasNext() {
			if(raf==null || !raf.getChannel().isOpen()) return false;
			
			if(flagNext == true) return true;	//Déjà lue
			
			flagNext = true;
			try {
				next = new Entry();
				next.timestamp = (long)raf.readInt();
				next.value = raf.readFloat();
				if(this.end!=null && next.timestamp > this.end){
					next = null;
					close();
					return false;
				}
				return true;
			}
			catch (IOException e) {
				//EOF ou autre erreur d'IO
				if(! (e instanceof EOFException)) logger.log(Level.WARNING,e.getMessage(),e);
				next = null;
				try {
					close();
				} catch (IOException e1) {
					logger.log(Level.WARNING,e.getMessage(),e);
				}
				return false;
			} 
		}

		public Entry next() {
			if(!flagNext) hasNext();
			if(next==null) throw new NoSuchElementException();
			flagNext = false;
			return next;
		}

		public void remove() {
			throw new RuntimeException("remove() non implemente");
		}
		
		public void close() throws IOException{
			if(raf!=null && raf.getChannel().isOpen()) raf.close();
		}
	}
}
