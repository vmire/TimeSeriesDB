package com.mireau.timeseries;

import java.text.DateFormat;
import java.text.NumberFormat;
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
	boolean smoothEstimation = false;

	/**
	 * @return json
	 */
	@Override
	public JsonObject toJson(DateFormat dateFormat, NumberFormat numberFormat){
		JsonObjectBuilder item = Json.createObjectBuilder();
		item.add("t", dateFormat.format(new Date(this.timestamp*1000)));
		if(this.value==null) item.add("v",JsonValue.NULL); else item.add("v", numberFormat.format(this.value));
		if(this.diff==null) item.add("diff",JsonValue.NULL); else item.add("diff", numberFormat.format(this.diff));
		if(this.overflow) item.add("ovf", JsonValue.TRUE);
		if(this.smoothEstimation) item.add("estimation", JsonValue.TRUE);
		return item.build();
	}
	
	@Override
	public String toCsvString(DateFormat dateFormat, NumberFormat numberFormat){
		return  dateFormat.format(new Date(this.timestamp*1000))
				+";"+(this.value==null ? "" : numberFormat.format(this.value))
				+";"+(this.diff==null ? "" : numberFormat.format(this.diff))
				+";"+(this.overflow ? "1":"0")
				+";"+(this.smoothEstimation ? "1":"0")
				;
	}
	
	
	
	public Float getDiff(){ return diff; }
	public boolean isOverflow() {
		return overflow;
	}
	public boolean isSmoothEstimation() {
		return smoothEstimation;
	}
}
