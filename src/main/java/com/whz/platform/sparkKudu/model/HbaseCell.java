package com.whz.platform.sparkKudu.model;

import java.io.Serializable;

public class HbaseCell implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String rowkey;
	private String family;
	private String column;
	private Object value;
	private long timestamp;
	
	public HbaseCell(){}
	
	public HbaseCell(String rowkey,String family,String column,Object value,long timestamp){
		this.rowkey = rowkey;
		this.family = family;
		this.column = column;
		this.value = value;
		this.timestamp = timestamp;
	}
	
	public String getRowkey() {
		return rowkey;
	}
	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}
	public String getFamily() {
		return family;
	}
	public void setFamily(String family) {
		this.family = family;
	}
	public String getColumn() {
		return column;
	}
	public void setColumn(String column) {
		this.column = column;
	}
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
}
