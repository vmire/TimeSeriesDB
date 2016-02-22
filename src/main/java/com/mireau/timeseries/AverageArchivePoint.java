package com.mireau.timeseries;

import java.util.Date;

import javax.json.Json;
import javax.json.JsonObjectBuilder;

public class AverageArchivePoint extends ArchivePoint {

	public AverageArchivePoint() {
		super();
	}
	
	Float min = null;
	Float max = null;
	

	/**
	 * @return json
	 */
	public String toJSONString(){
		JsonObjectBuilder item = Json.createObjectBuilder();
		item.add("t", jsonDateFormat.format(new Date(this.timestamp*1000)));
		item.add("v", this.value==null ? null : jsonNumberFormater.format(this.value));
		item.add("min", this.min==null ? null : jsonNumberFormater.format(this.min));
		item.add("max", this.max==null ? null : jsonNumberFormater.format(this.max));
		return item.build().toString();
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
