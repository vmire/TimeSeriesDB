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
	
	
	
	public Date getDate(){ 
		return new Date(this.timestamp*1000);
	}
	public Float getValue(){ return this.value; }
	
	
	abstract public JsonObject toJson(DateFormat dateFormat, NumberFormat numberFormat);
	abstract public String toCsvString(DateFormat dateFormat, NumberFormat numberFormat);
	
	public String toString(){
		NumberFormat numberFormat = DecimalFormat.getInstance(Locale.US);	//pour avoir des points en séparateur décimal;
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss");;
		numberFormat.setGroupingUsed(false);
		numberFormat.setRoundingMode(RoundingMode.HALF_DOWN);
		numberFormat.setMinimumFractionDigits(0);
		numberFormat.setMaximumFractionDigits(2);
		numberFormat.setMinimumIntegerDigits(0);
		numberFormat.setMaximumIntegerDigits(10);
		return toCsvString(dateFormat,numberFormat);
	}
}