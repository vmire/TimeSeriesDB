package com.mireau.timeseries;

import java.text.DateFormat;
import java.text.NumberFormat;
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
	public JsonObject toJson(DateFormat dateFormat, NumberFormat numberFormat){
		JsonObjectBuilder item = Json.createObjectBuilder();
		item.add("t", dateFormat.format(new Date(this.timestamp*1000)));
		if(this.value==null) item.add("v",JsonValue.NULL); else item.add("v",numberFormat.format(this.value));
		if(this.min==null) item.add("min",JsonValue.NULL); else item.add("min",numberFormat.format(this.min));
		if(this.max==null) item.add("max",JsonValue.NULL); else item.add("max",numberFormat.format(this.max));
		return item.build();
	}
	
	@Override
	public String toCsvString(DateFormat dateFormat, NumberFormat numberFormat){
		return  dateFormat.format(new Date(this.timestamp*1000))
				+";"+(this.value==null ? "" : numberFormat.format(this.value))
				+";"+(this.min==null ? "" : numberFormat.format(this.min))
				+";"+(this.max==null ? "" : numberFormat.format(this.max));
	}
	
	
	
	public Float getMin(){ return min; }
	public Float getMax(){ return max; }
}
