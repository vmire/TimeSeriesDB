package com.mireau.timeseries;

import java.util.Date;

import javax.json.Json;
import javax.json.JsonObjectBuilder;

public class AbsCounterArchivePoint extends ArchivePoint {

	public AbsCounterArchivePoint() {
		super();
	}
	
	Float diff = null;
	boolean overflow = false;
	

	/**
	 * @return json
	 */
	public String toJSONString(){
		JsonObjectBuilder item = Json.createObjectBuilder();
		item.add("t", jsonDateFormat.format(new Date(this.timestamp*1000)));
		item.add("v", this.value==null ? null : jsonNumberFormater.format(this.value));
		item.add("diff", this.diff==null ? null : jsonNumberFormater.format(this.diff));
		if(this.overflow) item.add("ovf", true);
		return item.build().toString();
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
