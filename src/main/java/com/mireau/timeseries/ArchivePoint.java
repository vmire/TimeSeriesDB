package com.mireau.timeseries;

import java.math.RoundingMode;
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
	public Float getValue(){ return this.value; }
	
	protected NumberFormat numberFormater;
	public ArchivePoint(){
		numberFormater = NumberFormat.getInstance();
		numberFormater.setGroupingUsed(false);
		numberFormater.setRoundingMode(RoundingMode.HALF_DOWN);
		numberFormater.setMinimumFractionDigits(0);
		numberFormater.setMaximumFractionDigits(5);
		numberFormater.setMinimumIntegerDigits(0);
		numberFormater.setMaximumIntegerDigits(10);
	}
	
	public String json(DateFormat dateFormat){
		return  "{t='"+dateFormat.format(new Date(this.timestamp))+"' v="+numberFormater.format(this.value)+"}";
	}
	
	public String csv(DateFormat dateFormat){
		return  dateFormat.format(new Date(this.timestamp*1000))
				+";"+(this.value==null ? "" : numberFormater.format(this.value));
	}
	public String toString(){
		return csv(new SimpleDateFormat("yyyyMMdd-HHmmss"));
	}
}