package com.stackroute.datamunger.query.parser;

import java.util.List;
import java.util.Map;

/* 
 * This class will contain the elements of the parsed Query String such as conditions,
 * logical operators,aggregate functions, file name, fields group by fields, order by
 * fields, Query Type
 * */
public class QueryParameter {

		private String queryString;
		private List<Restriction> restrictions;
		private List<String> logicalOperators;
		private List<AggregateFunction> aggregateFunctions;
		private String file;
		// query without where condition
		private String baseQuery;
		// Selected fields. If it is null -> * ( i.e., select all fields)
		private List<String> fields;
		private List<String> groupByFields;
		private List<String> orderByFields;
		// Query type may be simple, group by, order by, aggregate
		private String QUERY_TYPE = "SIMPLE_QUERY";
		// place holder for header
		private Map<String, Integer> header;
		public Map<String, Integer> getHeader() {
			return header;
		}
		public void setHeader(Map<String, Integer> header) {
			this.header = header;
		}
		public String getQUERY_TYPE() {
			return QUERY_TYPE;
		}
		public void setQUERY_TYPE(String qUERY_TYPE) {
			QUERY_TYPE = qUERY_TYPE;
		}
		public String getFile() {
			return file;
		}
		public void setFile(String file) {
			this.file = file;
		}
		public String getQueryString() {
			return queryString;
		}
		public void setQueryString(String queryString) {
			this.queryString = queryString;
		}
		public List<Restriction> getRestrictions() {
			return restrictions;
		}
		public void setRestrictions(List<Restriction> restrictions) {
			this.restrictions = restrictions;
		}
		public List<String> getLogicalOperators() {
			return logicalOperators;
		}
		public void setLogicalOperators(List<String> logicalOperators) {
			this.logicalOperators = logicalOperators;
		}
		public String getBaseQuery() {
			return baseQuery;
		}
		public void setBaseQuery(String baseQuery) {
			this.baseQuery = baseQuery;
		}
		public List<String> getFields() {
			return fields;
		}
		public void setFields(List<String> fields) {
			this.fields = fields;
		}
		public List<AggregateFunction> getAggregateFunctions() {
			return aggregateFunctions;
		}
		public void setAggregateFunctions(List<AggregateFunction> aggregateFunctions) {
			this.aggregateFunctions = aggregateFunctions;
		}
		public List<String> getGroupByFields() {
			return groupByFields;
		}
		public void setGroupByFields(List<String> groupByFields) {
			this.groupByFields = groupByFields;
		}
		public List<String> getOrderByFields() {
			return orderByFields;
		}
		public void setOrderByFields(List<String> orderByFields) {
			this.orderByFields = orderByFields;
		}

	
}
