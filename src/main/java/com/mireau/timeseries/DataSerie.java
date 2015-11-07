package com.mireau.timeseries;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

public abstract class DataSerie {
	
	static Logger logger = Logger.getLogger(DataSerie.class.getName());
	
	String id;
	File directory;
	String accessMode = "rw";
	
	protected DataSerie(File dir, String id){
		this.id = id;
		this.directory = dir;
	}
	
	public void setReadMode(){
		accessMode = "r";
	}
	public void setReadWriteMode(){
		accessMode = "rw";
	}
	
	public abstract int readInt() throws IOException;
	public abstract float readFloat() throws IOException;
	
	public abstract void moveTo(int timestamp) throws IOException;
	public abstract int moveToLast() throws IOException;
	public abstract void moveToFirst() throws IOException;
	//public abstract void next() throws IOException;
	
	public abstract boolean hasNext() throws IOException;
}
