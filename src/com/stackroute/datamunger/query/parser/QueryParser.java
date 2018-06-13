package com.stackroute.datamunger.query.parser;

import java.util.ArrayList;
import java.util.List;

public class QueryParser {

	private QueryParameter queryParameter = new QueryParameter();
	/*
	 * this method will parse the queryString and will return the object of
	 * QueryParameter class
	 */
	public QueryParameter parseQuery(String queryString) {
		queryParameter.setQueryString(queryString);
		queryParameter.setQUERY_TYPE("SIMPLE_QUERY");
		String baseQuery = queryString.split("where|ordery by|group by")[0].trim();
		queryParameter.setBaseQuery(baseQuery);
		/*
		 * extract the name of the file from the query. File name can be found after the
		 * "from" clause.
		 */
		String file = baseQuery.split("from")[1].trim().split("\\s+")[0];
		queryParameter.setFile(file.trim());
		queryParameter.setFields(getFields(baseQuery));
		queryParameter.setRestrictions(getRestrictions());
		queryParameter.setLogicalOperators(getLogicalOperators());
		
		queryParameter.setOrderByFields(getOrderByFields(queryString));
		queryParameter.setGroupByFields(getGroupByFields(queryString));
		queryParameter.setAggregateFunctions(getAggregateFunctions(queryString));
		return queryParameter;
	}
	/*
	 * extract the order by fields from the query string. Please note that we will
	 * need to extract the field(s) after "order by" clause in the query, if at all
	 * the order by clause exists. For eg: select city,winner,team1,team2 from
	 * data/ipl.csv order by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one order by fields.
	 */
	private List<String> getOrderByFields(String queryString) {
		List<String> orderByFieldList = null;
		if (hasOrderByField(queryString)) {
			String orderByFields[] = queryString.split("\\s+order by\\s+")[1].split("\\s+group by\\s+")[0].split(",");
			orderByFieldList = new ArrayList<>();
			for (String orderByField : orderByFields) {
				orderByFieldList.add(orderByField);
			}
		}
		return orderByFieldList;
	}
	
	/*
	 * extract the group by fields from the query string. Please note that we will
	 * need to extract the field(s) after "group by" clause in the query, if at all
	 * the group by clause exists. For eg: select city,max(win_by_runs) from
	 * data/ipl.csv group by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one group by fields.
	 */
	private List<String> getGroupByFields(String queryString) {
		List<String> groupByFieldList = null;
		if (hasGroupByField(queryString)) {
			String groupByFields[] = queryString.split("\\s+group by\\s+")[1].split("\\s+order by\\s+")[0].split(",");
			groupByFieldList = new ArrayList<>();
			for (String groupByField : groupByFields) {
				groupByFieldList.add(groupByField);
			}
		}
		return groupByFieldList;
	}
	private boolean hasOrderByField(String queryString) {
		if (queryString.contains("order by")) {
			if (queryParameter.getQUERY_TYPE().equals("GROUP_BY_QUERY")) {
				queryParameter.setQUERY_TYPE("GROUP_BY_ORDER_BY_QUERY");
			} else {
				queryParameter.setQUERY_TYPE("ORDER_BY_QUERY");
			}
			return true;
		} else {
			return false;
		}
	}
	private boolean hasGroupByField(String queryString) {
		if (queryString.contains("group by")) {
			if (queryParameter.getQUERY_TYPE().equals("ORDER_BY_QUERY")) {
				queryParameter.setQUERY_TYPE("GROUP_BY_ORDER_BY_QUERY");
			} else {
				queryParameter.setQUERY_TYPE("GROUP_BY_QUERY");
			}
			return true;
		} else {
			return false;
		}
	}
	
	/*
	 * extract the selected fields from the query string. Please note that we will
	 * need to extract the field(s) after "select" clause followed by a space from
	 * the query string. For eg: select city,win_by_runs from data/ipl.csv from the
	 * query mentioned above, we need to extract "city" and "win_by_runs". Please
	 * note that we might have a field containing name "from_date" or "from_hrs".
	 * Hence, consider this while parsing.
	 */
	private List<String> getFields(String baseQuery) {
		String[] fields = baseQuery.trim().split("select")[1].split("from")[0].trim().split(",");
		List<String> fieldList = new ArrayList<>();
		for (String field : fields) {
			fieldList.add(field.trim());
		}
		return fieldList;
	}
	
	/*
	 * extract the conditions from the query string(if exists). for each condition,
	 * we need to capture the following: 
	 * 1. Name of field 
	 * 2. condition 
	 * 3. value
	 * 
	 * For eg: select city,winner,team1,team2,player_of_match from data/ipl.csv
	 * where season >= 2008 or toss_decision != bat
	 * 
	 * here, for the first condition, "season>=2008" we need to capture: 
	 * 1. Name of field: season 
	 * 2. condition: >= 
	 * 3. value: 2008
	 * 
	 * the query might contain multiple conditions separated by OR/AND operators.
	 * Please consider this while parsing the conditions.
	 * 
	 */
	private List<Restriction> getRestrictions() {
		// select * from table where field='val'
		// extract where conditions
		String queryString = queryParameter.getQueryString();
		List<Restriction> restrictions = null;
		if (queryString.contains("where")) {
			queryParameter.setQUERY_TYPE("WHERE_CLAUSE_QUERY");
			String whereClauseQuery = queryString.split("where")[1].split("order by")[0].split("group by")[0];
			String[] expressions = whereClauseQuery.split("\\s+and\\s+|\\s+or\\s+");
			String propertyName;
			String propertyValue;
			String condition;
			Restriction restriction;
			String propertyNameAndValue[];
			restrictions = new ArrayList<Restriction>();
			if (whereClauseQuery != null) {
				for (String expression : expressions) {
					expression = expression.trim();
					propertyNameAndValue = expression.split("<=|>=|!=|<|>|=");
					propertyName = propertyNameAndValue[0].trim();
					propertyValue = propertyNameAndValue[1].trim().replace("'", "");
					// salary<10000
					condition = expression.split(propertyName)[1].trim().split(propertyValue)[0].trim().replace("'", "");
					restriction = new Restriction(propertyName, propertyValue, condition);
					restrictions.add(restriction);
				}
			}
		}
		return restrictions;
	}
	
	/*
	 * extract the logical operators(AND/OR) from the query, if at all it is
	 * present. For eg: select city,winner,team1,team2,player_of_match from
	 * data/ipl.csv where season >= 2008 or toss_decision != bat and city =
	 * bangalore
	 * 
	 * the query mentioned above in the example should return a List of Strings
	 * containing [or,and]
	 */
	private List<String> getLogicalOperators() {
		List<String> logicalOperators = null;
		String queryString = queryParameter.getQueryString();
		if (queryString.contains("where")) {
			String whereClauseQuery = queryString.split("where")[1].split("group by|order by")[0];
			String[] expressions = whereClauseQuery.split("\\s+and\\s+|\\s+or\\s+");
			logicalOperators = new ArrayList<String>();
			int size = expressions.length;
			int i = 0;
			for (String expression : expressions) {
				if (i++ < size - 1)
					logicalOperators.add(whereClauseQuery.split(expression.trim())[1].split("\\s+")[1].trim());
			}
		}
		return logicalOperators;
	}
	
	/*
	 * extract the aggregate functions from the query. The presence of the aggregate
	 * functions can determined if we have either "min" or "max" or "sum" or "count"
	 * or "avg" followed by opening braces"(" after "select" clause in the query
	 * string. in case it is present, then we will have to extract the same. For
	 * each aggregate functions, we need to know the following: 
	 * 1. type of aggregate function(min/max/count/sum/avg) 
	 * 2. field on which the aggregate function is being applied
	 * 
	 * Please note that more than one aggregate function can be present in a query
	 * 
	 * 
	 */
	private List<AggregateFunction> getAggregateFunctions(String queryString) {
		if (hasAggregateFunctions(queryString)) {
			queryString = queryString.trim();
			String aggregateFunctions[] = queryString.split("from")[0].split("select")[1].split(",");
			int size = aggregateFunctions.length;
			String aggregate;
			String function;
			String aggregateField;
			List<AggregateFunction> agregateFunctionList = new ArrayList<AggregateFunction>();
			AggregateFunction agregateFunction;
			for (int i = 0; i < size; i++) {
				aggregate = aggregateFunctions[i].trim();
				if (aggregate.contains("(")) {
					function = aggregate.split("\\(")[0].trim();
					aggregateField = aggregate.split("\\(")[1].trim().split("\\)")[0];
					agregateFunction = new AggregateFunction();
					agregateFunction.setField(aggregateField);
					agregateFunction.setFunction(function);
					agregateFunctionList.add(agregateFunction);
				}
			}
			return agregateFunctionList;
		}
		return null;
	}
	private boolean hasAggregateFunctions(String queryString) {
		if (queryString.contains("sum") || queryString.contains("min") || queryString.contains("max")
				|| queryString.contains("avg") || queryString.contains("count")) {
				if(hasGroupByField(queryString)) {
					queryParameter.setQUERY_TYPE("GROUP_BY_AGGREGATE_QUERY");
				} else {
					queryParameter.setQUERY_TYPE("AGGREGATE_QUERY");
				}
					
			return true;
		}
		return false;
	}
	
}
