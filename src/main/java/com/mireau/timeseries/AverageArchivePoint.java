package com.mireau.timeseries;

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
	public String json(){
		return  "{v="+numberFormater.format(this.value)+" min="+numberFormater.format(this.min)+" max="+numberFormater.format(this.max)+"}";
	}
	
	public String csv(){
		return  numberFormater.format(this.value)+";"+numberFormater.format(this.min)+";"+numberFormater.format(this.max);
	}
}
