package com.stackroute.datamunger.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

//this class will be acting as the DataSet containing multiple rows
public class DataSet extends LinkedHashMap<Long, Row> {

	private double sum;
	private long count;
	private String min;
	private String max;
	private double avg;

	public double getSum() {
		return sum;
	}

	public void setSum(double sum) {
		this.sum = sum;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public String getMin() {
		return min;
	}

	public void setMin(String min) {
		this.min = min;
	}

	public String getMax() {
		return max;
	}

	public void setMax(String max) {
		this.max = max;
	}

	public double getAvg() {
		return avg;
	}

	public void setAvg(double avg) {
		this.avg = avg;
	}

	/*
	 * The sort() method will sort the dataSet based on the key column with the help
	 * of Comparator
	 */
	public DataSet sort(String dataType, String columnName) {
		long rowId = 1;
		List<Row> dataSetList = new ArrayList<>(this.values());
		Collections.sort(dataSetList, new GenericComparator(columnName, dataType));
		DataSet sortedDataSet = new DataSet();
		for (Row rowData : dataSetList) {
			sortedDataSet.put(rowId, rowData);
			rowId++;
		}
		return sortedDataSet;
	}

}
