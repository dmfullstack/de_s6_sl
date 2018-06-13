package com.stackroute.datamunger.query;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/*
 * implementation of DataTypeDefinitions class. This class contains a method getDataTypes() 
 * which will contain the logic for getting the datatype for a given field value. This
 * method will be called from QueryProcessors.   
 * In this assignment, we are going to use Regular Expression to find the 
 * appropriate data type of a field. 
 * Integers: should contain only digits without decimal point 
 * Double: should contain digits as well as decimal point 
 * Date: Dates can be written in many formats in the CSV file. 
 * However, in this assignment,we will test for the following date formats('dd/mm/yyyy',
 * 'mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','yyyy-mm-dd')
 */
public class DataTypeDefinitions {

	public static Object getDataType(String input) {
		if (input.isEmpty()) {
			return new Object();
		}
		// checking for Integer
		else if (input.matches("^[0-9]+$"))
			return getIntegerValue(input);
		// checking for floating point numbers
		else if (input.matches("^[0-9\\.]*"))
			return getDoubleValue(input);
		// checking for date format dd/mm/yyyy
		else if (input.matches("^[0-3]{1}[0-9]{1}\\/[0-1]{1}[0-9]{1}\\/[0-9]{4}"))
			return getDateValue(input, "dd/MM/yyyy");
		// checking for date format mm/dd/yyyy
		else if (input.matches("^[0-1]{1}[0-9]{1}\\/[0-3]{1}[0-9]{1}\\/[0-9]{4}"))
			return getDateValue(input, "MM/dd/yyyy");
		// checking for date format dd-mon-yy
		else if (input.matches("^[0-9]{2}-[a-zA-Z]{3}-[0-9]{2}"))
			return getDateValue(input, "dd-MMM-yy");
		// checking for date format dd-mon-yyyy
		else if (input.matches("^[0-9]{2}-[a-zA-Z]{3}-[0-9]{4}"))
			return getDateValue(input, "dd-MMM-yyyy");
		// checking for date format dd-month-yy
		else if (input.matches("^[0-9]{2}-[a-zA-Z]{3}[a-z]+-[0-9]{2}"))
			return getDateValue(input, "dd-MMMM-yy");
		// checking for date format dd-month-yyyy
		else if (input.matches("^[0-9]{2}-[a-zA-Z]{3}[a-z]+-[0-9]{4}"))
			return getDateValue(input, "dd-MMMM-yyyy");
		else
			return input;
	}
	public static Integer getIntegerValue(String text) {
		Integer number = Integer.parseInt(text);
		return number;
	}
	public static Double getDoubleValue(String text) {
		Double number = Double.parseDouble(text);
		return number;
	}
	public static Date getDateValue(String text, String dateFormat) {
		DateFormat df = new SimpleDateFormat(dateFormat);
		Date date = null;
		try {
			date = df.parse(text);
		} catch (ParseException pe) {
			pe.printStackTrace();
		}
		return date;
	}

	
}
