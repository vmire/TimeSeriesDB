package com.mireau.timeseries;

import java.util.Date;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

public class AbsCounterArchivePoint extends ArchivePoint {

	public AbsCounterArchivePoint() {
		super();
	}
	
	Float diff = null;
	boolean overflow = false;
	

	/**
	 * @return json
	 */
	@Override
	public JsonObject toJson(){
		JsonObjectBuilder item = Json.createObjectBuilder();
		item.add("t", jsonDateFormat.format(new Date(this.timestamp*1000)));
		if(this.value==null) item.add("v",JsonValue.NULL); else item.add("v", jsonNumberFormater.format(this.value));
		if(this.diff==null) item.add("diff",JsonValue.NULL); else item.add("diff", jsonNumberFormater.format(this.diff));
		if(this.overflow) item.add("ovf", JsonValue.TRUE);
		return item.build();
	}
	
	@Override
	public String toCSVString(){
		return  jsonDateFormat.format(new Date(this.timestamp*1000))
				+";"+(this.value==null ? "" : jsonNumberFormater.format(this.value))
				+";"+(this.diff==null ? "" : jsonNumberFormater.format(this.diff))
				+";"+(this.overflow ? "1":"0")
				;
	}
	
	
	
	public Float getDiff(){ return diff; }
	public boolean isOverflow() {
		return overflow;
	}
	
}
