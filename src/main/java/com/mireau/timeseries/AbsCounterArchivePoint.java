package com.mireau.timeseries;

import java.text.DateFormat;
import java.text.NumberFormat;
import java.util.Date;

public class AbsCounterArchivePoint extends ArchivePoint {

	public AbsCounterArchivePoint() {
		super();
	}
	
	Float diff = null;
	boolean overflow = false;
	

	/**
	 * @override
	 * @return json
	 */
	@Override
	public String json(DateFormat dateFormat, NumberFormat numberFormater){
		return  "{"
				+"t:'"+dateFormat.format(new Date(this.timestamp*1000))+"'"
				+", v:"+(this.value==null ? "" : numberFormater.format(this.value))
				+", diff:"+(this.diff==null ? "" : numberFormater.format(this.diff))
				+", ovf:"+(this.overflow ? "1":"0")
				+"}";
	}
	
	@Override
	public String csv(DateFormat dateFormat, NumberFormat numberFormater){
		return  dateFormat.format(new Date(this.timestamp*1000))
				+";"+(this.value==null ? "" : numberFormater.format(this.value))
				+";"+(this.diff==null ? "" : numberFormater.format(this.diff))
				+";"+(this.overflow ? "1":"0")
				;
	}
	
	
	
	public Float getDiff(){ return diff; }
	public boolean isOverflow() {
		return overflow;
	}
	
}
