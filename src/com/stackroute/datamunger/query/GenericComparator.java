package com.stackroute.datamunger.query;

import java.util.Comparator;

/*
 * The GenericComparator class implements Comparator Interface. This class is used to 
 * compare row objects which will be used for sorting the dataSet
 */
public class GenericComparator implements Comparator<Row> {
	private final String columnName;
	private final String dataType;

	public GenericComparator(String columnName, String dataType) {
		super();
		this.columnName = columnName;
		this.dataType = dataType;
	}

	@Override
	public int compare(Row o1, Row o2) {
		if(dataType.equalsIgnoreCase("java.lang.Integer")) {
			if((Integer.parseInt(o1.get(columnName)))== (Integer.parseInt(o2.get(columnName)))){
                return 0;
          }else if((Integer.parseInt(o1.get(columnName))) > (Integer.parseInt(o2.get(columnName)))) {
                return 1;
          }else {
                return -1;
          }
		}
		else if(dataType.equalsIgnoreCase("java.lang.Double")) {
			if((Double.parseDouble(o1.get(columnName)))== (Double.parseDouble(o2.get(columnName)))){
                return 0;
          }else if((Double.parseDouble(o1.get(columnName))) > (Double.parseDouble(o2.get(columnName)))) {
                return 1;
          }else {
                return -1;
          }
		} 
		
		return o1.get(columnName).toString().compareTo(o2.get(columnName).toString());
	}
}
