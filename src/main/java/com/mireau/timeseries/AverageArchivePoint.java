package com.mireau.timeseries;

import java.text.DateFormat;
import java.text.NumberFormat;

public class AverageArchivePoint extends ArchivePoint {

	public AverageArchivePoint() {
		super();
	}
	
	Float min = null;
	Float max = null;
	

	/**
	 * @override
	 * @return json
	 */
	@Override
	public String json(DateFormat dateFormat, NumberFormat numberFormater){
		return  "{"
				+"\"t\":\""+dateFormat.format(getDate())+"\""
				+", \"v\":"+(this.value==null ? null : "\""+numberFormater.format(this.value)+"\"")
				+", \"min\":"+(this.min==null ? null : "\""+numberFormater.format(this.min)+"\"")
				+", \"max\":"+(this.max==null ? null : "\""+numberFormater.format(this.max)+"\"")
				+"}";
	}
	
	@Override
	public String csv(DateFormat dateFormat, NumberFormat numberFormater){
		return  dateFormat.format(getDate())
				+";"+(this.value==null ? "" : numberFormater.format(this.value))
				+";"+(this.min==null ? "" : numberFormater.format(this.min))
				+";"+(this.max==null ? "" : numberFormater.format(this.max));
	}
	
	
	
	public Float getMin(){ return min; }
	public Float getMax(){ return max; }
}
