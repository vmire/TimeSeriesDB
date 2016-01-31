package com.mireau.timeseries;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class Meta {

	String label;
	Archive.Type type;
	String unit;
	File metadataFile = null;
	
	public Meta(File f) {
		this.metadataFile = f;
	}

	/**
	 * Lit les données du fichier de méta données
	 * @throws IOException
	 */
	public void readMetadata() throws IOException{
		if(metadataFile != null && metadataFile.exists()){
			//Le fichier de méta données existe
			Properties props = new Properties();
			InputStream is = null;
			try{
				is = new FileInputStream(metadataFile);
				props.load(is);
			}
			finally{
				if(is!=null) is.close();
			}
			this.unit = props.getProperty("unit","");
			this.label = props.getProperty("label","");
			String typeStr = props.getProperty("type");
			if(typeStr!=null) this.type = Archive.decodeType(typeStr);
		}
	}
	
	public void writeMetadata() throws IOException{
		Properties props = new Properties();
		if(label != null) props.put("label", this.label);
		if(unit != null) props.put("unit", this.unit);
		if(type != null) props.put("type", this.type.toString());
		OutputStream out = null;
		try{
			out = new FileOutputStream(metadataFile);
			props.store(out, this.metadataFile.getName());
		}
		finally{
			if(out!=null) out.close();
		}
	}
	
	public String getLabel() {
		return label;
	}

	public Archive.Type getType() {
		return type;
	}

	public String getUnit() {
		return unit;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public void setType(Archive.Type type) {
		this.type = type;
	}

	public void setUnit(String unit) {
		this.unit = unit;
	}
}
