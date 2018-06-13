package com.stackroute.datamunger.query;

import java.io.FileNotFoundException;
import java.util.HashMap;

import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.QueryParser;
import com.stackroute.datamunger.reader.CsvAggregateQueryProcessor;
import com.stackroute.datamunger.reader.CsvGroupByAggregateQueryProcessor;
import com.stackroute.datamunger.reader.CsvGroupByQueryProcessor;
import com.stackroute.datamunger.reader.CsvQueryProcessor;
import com.stackroute.datamunger.reader.QueryProcessingEngine;

public class Query {

	QueryParser queryParser = null;
	QueryParameter queryParameter = null;
	
	/*
	 * This method will: 
	 * 1.parse the query and populate the QueryParameter object
	 * 2.Based on the type of query, it will select the appropriate Query processor.
	 * In this example, we are going to work with only one Query Processor which is
	 * CsvQueryProcessor, which can work with select queries containing zero, one or
	 * multiple conditions
	 */
	public HashMap executeQuery(String queryString)  throws FileNotFoundException {
		/* instantiate QueryParser class */
		queryParser = new QueryParser();
		/*
		 * call parseQuery() method of the class by passing the queryString which will
		 * return object of QueryParameter
		 */
		queryParameter = queryParser.parseQuery(queryString);
		QueryProcessingEngine queryEngine = null;

		/*
		 * Check for Type of Query based on the QueryParameter object. In this
		 * assignment, we will process queries containing zero, one or multiple
		 * where conditions i.e. conditions, aggregate functions, order by, group by clause
		 */
		switch (queryParameter.getQUERY_TYPE()) {
		// queries without aggregate functions, order by clause or group by clause
		case "SIMPLE_QUERY":
		case "WHERE_CLAUSE_QUERY":
		case "ORDER_BY_QUERY":
			queryEngine = new CsvQueryProcessor();
			break;
		//queries with aggregate functions
		case "AGGREGATE_QUERY":
			queryEngine=new CsvAggregateQueryProcessor();
			break;
		//Queries with group by clause
		case "GROUP_BY_ORDER_BY_QUERY":
		case "GROUP_BY_QUERY":
			queryEngine=new CsvGroupByQueryProcessor();
			break;
			
		case "GROUP_BY_AGGREGATE_QUERY":
			queryEngine=new CsvGroupByAggregateQueryProcessor();
			break;

		}
		/*
		 * call the getResultSet() method of CsvQueryProcessor class by passing the
		 * QueryParameter Object to it. This method is supposed to return resultSet
		 * which is a HashMap
		 */
		return queryEngine.getResultSet(queryParameter);
	}

}
