package com.stackroute.datamunger.query.parser;

/* This class is used for storing name of field, aggregate function for 
 * each aggregate function
 * */
public class AggregateFunction {
	
	private String field;
	private String result="0";
	private String function;
	private int aggregateFieldIndex;
	private int count;
	private double sum;
	public int getAggregateFieldIndex() {
		return aggregateFieldIndex;
	}
	public void setAggregateFieldIndex(int aggregateFieldIndex) {
		this.aggregateFieldIndex = aggregateFieldIndex;
	}
	public String getField() {
		return field;
	}
	public void setField(String field) {
		this.field = field;
	}
	public String getResult() {
		return result;
	}
	public void setResult(String result) {
		this.result = result;
	}
	public String getFunction() {
		return function;
	}
	public void setFunction(String function) {
		this.function = function;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public double getSum() {
		return sum;
	}
	public void setSum(double sum) {
		this.sum = sum;
	}
	
	
	

}
