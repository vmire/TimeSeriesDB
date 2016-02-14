package com.mireau.timeseries;

import java.util.Date;

import org.json.simple.JSONObject;

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
	public String toJSONString(){
		JSONObject o = new JSONObject();
		o.put("t", jsonDateFormat.format(new Date(this.timestamp*1000)));
		o.put("v", this.value==null ? null : jsonNumberFormater.format(this.value));
		o.put("min", this.min==null ? null : jsonNumberFormater.format(this.min));
		o.put("max", this.max==null ? null : jsonNumberFormater.format(this.max));
		return o.toJSONString();
	}
	
	@Override
	public String toCSVString(){
		return  jsonDateFormat.format(new Date(this.timestamp*1000))
				+";"+(this.value==null ? "" : jsonNumberFormater.format(this.value))
				+";"+(this.min==null ? "" : jsonNumberFormater.format(this.min))
				+";"+(this.max==null ? "" : jsonNumberFormater.format(this.max));
	}
	
	
	
	public Float getMin(){ return min; }
	public Float getMax(){ return max; }
}
