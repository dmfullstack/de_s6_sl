package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.GroupedDataSet;
import com.stackroute.datamunger.query.Header;
import com.stackroute.datamunger.query.Row;
import com.stackroute.datamunger.query.RowDataTypeDefinitions;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.Restriction;


//this class will read from CSV file and process and return the resultSet
public class CsvGroupByAggregateQueryProcessor implements QueryProcessingEngine {
	/*
	 * This method will take QueryParameter object as a parameter which contains the
	 * parsed query and will process and populate the ResultSet
	 */
	public GroupedDataSet getResultSet(QueryParameter queryParameter) {
		BufferedReader reader = null;
		long rowid = 1;
		String[] headers = null;
		String[] firstRowValues = null;
		String rowValue = null;
		String[] rowValues = null;
		DataSet dataSet = new DataSet();
		Header header = new Header();
		Row row = new Row();
		RowDataTypeDefinitions dataTypeDef = new RowDataTypeDefinitions();
		Filter filter = new Filter();
		int columnSequence = 1;
		GroupedDataSet groupedDataSet=new GroupedDataSet();
		int count=0;
		double sum=0.0;
		String min;
		String max;
		double avg=0.0;
		
		/*
		 * initialize BufferedReader to read from the file which is mentioned in
		 * QueryParameter. Consider Handling Exception related to file reading.
		 */
		try {
			reader = new BufferedReader(new FileReader(queryParameter.getFile()));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			/*
			 * read the first line which contains the header. Please note that the headers
			 * can contain spaces in between them. For eg: city, winner
			 */
			headers = reader.readLine().toLowerCase().split("\\s*,\\s*");
			/*
			 * read the next line which contains the first row of data. We are reading this
			 * line so that we can determine the data types of all the fields. Please note
			 * that ipl.csv file contains null value in the last column. If you do not
			 * consider this while splitting, this might cause exceptions later
			 */
			firstRowValues = reader.readLine().toLowerCase().split("\\s*,\\s*",-1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int colIndex = 0; colIndex < headers.length; colIndex++) {
			/*
			 * populate the header Map object from the header array. header map is having
			 * data type <String,Integer> to contain the header and it's index.
			 */
			header.put(headers[colIndex], colIndex);
			
			/*
			 * We have read the first line of text already and kept it in an array. Now, we
			 * can populate the dataTypeDefinition Map object. dataTypeDefinition map is
			 * having data type <Integer,String> to contain the index of the field and it's
			 * data type. To find the dataType by the field value, we will use getDataType()
			 * method of DataTypeDefinitions class
			 */
			dataTypeDef.put(colIndex, DataTypeDefinitions.getDataType(firstRowValues[colIndex]).getClass().getName());
		}
		/*
		 * once we have the header and dataTypeDefinitions maps populated, we can start
		 * reading from the first line. We will read one line at a time, then check
		 * whether the field values satisfy the conditions mentioned in the query,if
		 * yes, then we will add it to the resultSet. Otherwise, we will continue to
		 * read the next line. We will continue this till we have read till the last
		 * line of the CSV file.
		 */
		try {
			boolean isSelected;
			/* reset the buffered reader so that it can start reading from the first line */
			reader=new BufferedReader(new FileReader(queryParameter.getFile()));
			/*
			 * skip the first line as it is already read earlier which contained the header
			 */
			reader.readLine();
			/* read one line at a time from the CSV file till we have any lines left */
			while ((rowValue = reader.readLine()) != null) {
				/*
				 * once we have read one line, we will split it into a String Array. This array
				 * will continue all the fields of the row. Please note that fields might
				 * contain spaces in between. Also, few fields might be empty.
				 */
				rowValues = rowValue.split("\\s*,\\s*",-1);
				isSelected = true;
				
				/*
				 * if there are where condition(s) in the query, test the row fields against
				 * those conditions to check whether the selected row satifies the conditions
				 */
				if (queryParameter.getRestrictions() != null) {
					int iteration = 0;
					/*
					 * from QueryParameter object, read one condition at a time and evaluate the
					 * same. For evaluating the conditions, we will use evaluateExpressions() method
					 * of Filter class. Please note that evaluation of expression will be done
					 * differently based on the data type of the field. In case the query is having
					 * multiple conditions, you need to evaluate the overall expression i.e. if we
					 * have OR operator between two conditions, then the row will be selected if any
					 * of the condition is satisfied. However, in case of AND operator, the row will
					 * be selected only if both of them are satisfied.
					 */
					for (Restriction restriction : queryParameter.getRestrictions()) {
						if (iteration == 0) {
							isSelected = filter.evaluateExpression(restriction.getCondition(),
									rowValues[header.get(restriction.getPropertyName())],
									restriction.getPropertyValue(),
									dataTypeDef.get(header.get(restriction.getPropertyName())));
							continue;
						}
						
						/*
						 * check for multiple conditions in where clause for eg: where salary>20000 and
						 * city=Bangalore for eg: where salary>20000 or city=Bangalore and dept!=Sales
						 */
						
						if (queryParameter.getLogicalOperators().get(iteration) != null) {
							if (queryParameter.getLogicalOperators().get(iteration).equalsIgnoreCase("and")) {
								isSelected = isSelected && filter.evaluateExpression(restriction.getCondition(),
										rowValues[header.get(restriction.getPropertyName())],
										restriction.getPropertyValue(),
										dataTypeDef.get(header.get(restriction.getPropertyName())));
							}
							else if (queryParameter.getLogicalOperators().get(iteration).equalsIgnoreCase("or")) {
								isSelected = isSelected || filter.evaluateExpression(restriction.getCondition(),
										rowValues[header.get(restriction.getPropertyName())],
										restriction.getPropertyValue(),
										dataTypeDef.get(header.get(restriction.getPropertyName())));
							}
						}
					}
				}
				/*
				 * if the overall condition expression evaluates to true, then we need to check
				 * for the existence for group by clause in the Query Parameter. 
				 * The dataSet generated after processing a group by clause is completely different from 
				 * a dataSet structure(which contains multiple rows of data). In case of queries containing 
				 * group by clause, the resultSet will contain multiple dataSets, each of which will be assigned 
				 * to the group by column value i.e. for all unique values of the group by column,
				 * there can be multiple rows associated with it.
				 * Hence, we will use GroupedDataSet<String,Object> to store the same and not DataSet<Long,Row>.
				 * Please note we will process queries containing one group by column only
				 * for this example.
				 */
				if (isSelected) {
					
					row = new Row();
					// checking if the group by column value is there in the map
					if (!groupedDataSet.containsKey(rowValues[header.get(queryParameter.getGroupByFields().get(0))])) {
						// add the key and the dataset to the groupedDataSet
						groupedDataSet.put(rowValues[header.get(queryParameter.getGroupByFields().get(0))],
								new DataSet());
					}
					

					if (groupedDataSet.containsKey(rowValues[header.get(queryParameter.getGroupByFields().get(0))])) {
						dataSet = (DataSet) groupedDataSet
								.get(rowValues[header.get(queryParameter.getGroupByFields().get(0))]);
						for (int i = 0; i < rowValues.length; i++)
							row.put(headers[i], rowValues[i]);
						AggregateFunction aggregateFunction = queryParameter.getAggregateFunctions().get(0);
						if (aggregateFunction.getFunction().equals("count")) {
							if (aggregateFunction.getField().equals("*")
									|| !rowValues[header.get(aggregateFunction.getField())].isEmpty()) {
								dataSet.setCount(dataSet.getCount() + 1);
							}
						} else if (aggregateFunction.getFunction().equals("sum")) {
							if (!rowValues[header.get(aggregateFunction.getField())].isEmpty()) {
								dataSet.setSum(filter.sum(dataSet.getSum(),
										rowValues[header.get(aggregateFunction.getField())],
										dataTypeDef.get(header.get(aggregateFunction.getField()))));
							}
						} else if (aggregateFunction.getFunction().equals("min")) {
							if (!rowValues[header.get(aggregateFunction.getField())].isEmpty()) {
								min = dataSet.getMin();
								if (filter.lessThan(min, rowValues[header.get(aggregateFunction.getField())],
										dataTypeDef.get(header.get(aggregateFunction.getField())))) {
									dataSet.setMin(min);
								} else
									dataSet.setMin(rowValues[header.get(aggregateFunction.getField())]);
							}
						} else if (aggregateFunction.getFunction().equals("max")) {
							if (!rowValues[header.get(aggregateFunction.getField())].isEmpty()) {
								max = dataSet.getMax();
								if (filter.greaterThan(max, rowValues[header.get(aggregateFunction.getField())],
										dataTypeDef.get(header.get(aggregateFunction.getField())))) {
									dataSet.setMax(max);
								} else
									dataSet.setMax(rowValues[header.get(aggregateFunction.getField())]);
							}
						} else if (aggregateFunction.getFunction().equals("avg")) {
							if (!rowValues[header.get(aggregateFunction.getField())].isEmpty()) {
								
								dataSet.setSum(filter.sum(dataSet.getSum(),
										rowValues[header.get(aggregateFunction.getField())],
										dataTypeDef.get(header.get(aggregateFunction.getField()))));
								
								dataSet.setCount(dataSet.getCount() + 1);
								dataSet.setAvg(dataSet.getSum()/dataSet.getCount());
								
								
							}
						}
						
					}		
				}			
				rowid++;
			}
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		for (String key : groupedDataSet.keySet()) {
			DataSet resultSet = (DataSet) groupedDataSet.get(key);
			if (queryParameter.getAggregateFunctions().get(0).getFunction().equalsIgnoreCase("count"))
				groupedDataSet.put(key, resultSet.getCount());
			else if (queryParameter.getAggregateFunctions().get(0).getFunction().equalsIgnoreCase("sum"))
				groupedDataSet.put(key, resultSet.getSum());
			else if (queryParameter.getAggregateFunctions().get(0).getFunction().equalsIgnoreCase("min"))
				groupedDataSet.put(key, resultSet.getMin());
			else if (queryParameter.getAggregateFunctions().get(0).getFunction().equalsIgnoreCase("max"))
				groupedDataSet.put(key, resultSet.getMax());
			else if (queryParameter.getAggregateFunctions().get(0).getFunction().equalsIgnoreCase("avg"))
				groupedDataSet.put(key, resultSet.getAvg());
		}
		//return groupedDataSet object
		return groupedDataSet;
	}
	
	
	
	
	
}
