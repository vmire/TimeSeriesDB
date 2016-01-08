package com.mireau.timeseries;

import java.text.DateFormat;
import java.util.Date;

public class AverageArchivePoint extends ArchivePoint {

	public AverageArchivePoint() {
		super();
	}
	
	Float min = null;
	Float max = null;
	public Float getMin(){ return min; }
	public Float getMax(){ return max; }

	/**
	 * @override
	 * @return json
	 */
	@Override
	public String json(DateFormat dateFormat){
		return  "{t='"+dateFormat.format(new Date(this.timestamp))+"' v="+numberFormater.format(this.value)+" min="+numberFormater.format(this.min)+" max="+numberFormater.format(this.max)+"}";
	}
	
	@Override
	public String csv(DateFormat dateFormat){
		return  dateFormat.format(new Date(this.timestamp*1000))
				+";"+(this.value==null ? "" : numberFormater.format(this.value))
				+";"+(this.min==null ? "" : numberFormater.format(this.min))
				+";"+(this.max==null ? "" : numberFormater.format(this.max));
	}
}
