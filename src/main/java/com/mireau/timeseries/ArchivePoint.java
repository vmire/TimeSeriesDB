package com.mireau.timeseries;

import java.math.RoundingMode;
import java.text.NumberFormat;

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
	public String json(){
		return  "{v="+numberFormater.format(this.value)+"}";
	}
	public String csv(){
		return  String.valueOf(this.value);
	}
}