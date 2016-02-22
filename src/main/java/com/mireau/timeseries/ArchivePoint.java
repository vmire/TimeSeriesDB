package com.mireau.timeseries;

import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import javax.json.JsonObject;

/**
 * Classe qui represente un point de l'archive
 */
public abstract class ArchivePoint{
	Float value = null;
	long timestamp;
	
	static NumberFormat jsonNumberFormater;
	static DateFormat jsonDateFormat;
	
	static{
		jsonNumberFormater = DecimalFormat.getInstance(Locale.US);	//pour avoir des points en séparateur décimal
		jsonNumberFormater.setGroupingUsed(false);
		jsonNumberFormater.setRoundingMode(RoundingMode.HALF_DOWN);
		jsonNumberFormater.setMinimumFractionDigits(0);
		jsonNumberFormater.setMaximumFractionDigits(2);
		jsonNumberFormater.setMinimumIntegerDigits(0);
		jsonNumberFormater.setMaximumIntegerDigits(10);
		jsonDateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss");
	}
	
	public Date getDate(){ 
		return new Date(this.timestamp*1000);
	}
	public Float getValue(){ return this.value; }
	
	
	abstract public JsonObject toJson();
	abstract public String toCSVString();
	
	public String toString(){
		return toCSVString();
	}
}