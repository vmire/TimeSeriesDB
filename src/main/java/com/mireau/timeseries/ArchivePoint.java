package com.mireau.timeseries;

import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Classe qui represente un point de l'archive
 */
public class ArchivePoint{
	Float value = null;
	long timestamp;
	
	public Date getDate(){ 
		return new Date(this.timestamp*1000);
	}
	public Float getValue(){ return this.value; }
	
	
	public String json(DateFormat dateFormat, NumberFormat numberFormater){
		return  "{t='"+dateFormat.format(getDate())+"' v="+numberFormater.format(this.value)+"}";
	}
	
	public String csv(DateFormat dateFormat, NumberFormat numberFormater){
		return  dateFormat.format(getDate())
				+";"+(this.value==null ? "" : numberFormater.format(this.value));
	}
	public String toString(){
		return csv(new SimpleDateFormat("yyyyMMdd-HHmmss"), NumberFormat.getInstance());
	}
}