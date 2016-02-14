package com.mireau.timeseries;

import java.util.Date;

import org.json.simple.JSONObject;

public class AbsCounterArchivePoint extends ArchivePoint {

	public AbsCounterArchivePoint() {
		super();
	}
	
	Float diff = null;
	boolean overflow = false;
	

	/**
	 * @override
	 * @return json
	 */
	@Override
	public String toJSONString(){
		JSONObject o = new JSONObject();
		o.put("t", jsonDateFormat.format(new Date(this.timestamp*1000)));
		o.put("v", this.value==null ? null : jsonNumberFormater.format(this.value));
		o.put("diff", this.diff==null ? null : jsonNumberFormater.format(this.diff));
		if(this.overflow) o.put("ovf", true);
		return o.toJSONString();
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
