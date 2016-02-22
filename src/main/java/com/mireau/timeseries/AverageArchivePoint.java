package com.mireau.timeseries;

import java.util.Date;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

public class AverageArchivePoint extends ArchivePoint {

	public AverageArchivePoint() {
		super();
	}
	
	Float min = null;
	Float max = null;
	

	/**
	 * @return json
	 */
	@Override
	public JsonObject toJson(){
		JsonObjectBuilder item = Json.createObjectBuilder();
		item.add("t", jsonDateFormat.format(new Date(this.timestamp*1000)));
		if(this.value==null) item.add("v",JsonValue.NULL); else item.add("v",jsonNumberFormater.format(this.value));
		if(this.min==null) item.add("min",JsonValue.NULL); else item.add("min",jsonNumberFormater.format(this.min));
		if(this.max==null) item.add("max",JsonValue.NULL); else item.add("max",jsonNumberFormater.format(this.max));
		return item.build();
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
