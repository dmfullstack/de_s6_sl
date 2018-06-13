package com.stackroute.datamunger.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.stackroute.datamunger.query.Query;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.QueryParser;
import com.stackroute.datamunger.query.parser.Restriction;
import com.stackroute.datamunger.writer.JsonWriter;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DataMungerTest {

	private static Query query;

	private static QueryParser queryParser;
	private static QueryParameter queryParameter;
	private static JsonWriter jsonWriter;
	private String queryString;

	@Before
	public void setup() {
		// setup methods runs, before every test case runs
		// This method is used to initialize the required variables
		query = new Query();
		queryParser = new QueryParser();
		jsonWriter = new JsonWriter();
	}

	@After
	public void teardown() {
		// teardown method runs, after every test case run
		// This method is used to clear the initialized variables
		query = null;
		queryParser = null;
		jsonWriter = null;

	}

	/*
	 * The following test cases are used to check whether the parsing is working
	 * properly or not
	 */

	@Test
	public void testGetFileName() {
		queryString = "select * from data/ipl.csv";
		queryParameter = queryParser.parseQuery(queryString);
		assertEquals(
				"testGetFileName(): File name extraction failed. Check getFile() method. File name can be found after a space after from clause. Note: CSV file can contain a field that contains from as a part of the column name. For eg: from_date,from_hrs etc",
				"data/ipl.csv", queryParameter.getFile());
		display(queryString, queryParameter);
	}

	@Test
	public void testGetFileNameFailure() {
		queryString = "select * from data/ipl1.csv";
		queryParameter = queryParser.parseQuery(queryString);
		assertNotEquals(
				"testGetFileNameFailure(): File name extraction failed. Check getFile() method. File name can be found after a space after from clause. Note: CSV file can contain a field that contains from as a part of the column name. For eg: from_date,from_hrs etc",
				"data/ipl.csv", queryParameter.getFile());
		display(queryString, queryParameter);
	}

	@Test
	public void testGetFields() {
		queryString = "select city, winner, team1,team2 from data/ipl.csv";
		queryParameter = queryParser.parseQuery(queryString);
		List<String> expectedFields = new ArrayList<>();
		expectedFields.add("city");
		expectedFields.add("winner");
		expectedFields.add("team1");
		expectedFields.add("team2");
		assertArrayEquals(
				"testGetFields() : Select fields extractions failed. The query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				expectedFields.toArray(), queryParameter.getFields().toArray());
		display(queryString, queryParameter);
	}

	@Test
	public void testGetFieldsFailure() {
		queryString = "select city, winner, team1,team2 from data/ipl.csv";
		queryParameter = queryParser.parseQuery(queryString);
		assertNotNull(
				"testGetFieldsFailure() : Invalid Column / Field values. Please note that the query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				queryParameter.getFields());
		display(queryString, queryParameter);
	}

	@Test
	public void testGetFieldsAndRestrictions() {
		queryString = "select city,winner,player_of_match from data/ipl.csv where season > 2014";
		queryParameter = queryParser.parseQuery(queryString);

		List<Restriction> restrictions = queryParameter.getRestrictions();
		List<String> fields = new ArrayList<String>();
		fields.add("city");
		fields.add("winner");
		fields.add("player_of_match");

		assertEquals(
				"testGetFieldsAndRestrictions() : File name extraction failed. Check getFile() method. File name can be found after a space after from clause. Note: CSV file can contain a field that contains from as a part of the column name. For eg: from_date,from_hrs etc",
				"data/ipl.csv", queryParameter.getFile());
		assertEquals(
				"testGetFieldsAndRestrictions() : Select fields extractions failed. The query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				fields, queryParameter.getFields());
		assertEquals(
				"testGetFieldsAndRestrictions() : Retrieval of Base Query failed. BaseQuery contains from the beginning of the query till the where clause",
				"select city,winner,player_of_match from data/ipl.csv", queryParameter.getBaseQuery());
		assertTrue(
				"testGetFieldsAndRestrictions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string. ",
				restrictions.get(0).getPropertyName().contains("season"));
		assertTrue(
				"testGetFieldsAndRestrictions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string. ",
				restrictions.get(0).getPropertyValue().contains("2014"));
		assertTrue(
				"testGetFieldsAndRestrictions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string. ",
				restrictions.get(0).getCondition().contains(">"));

		display(queryString, queryParameter);
	}

	@Test
	public void testGetFieldsAndRestrictionsFailure() {
		queryString = "select city,winner,player_of_match from data/ipl.csv where season > 2014";
		queryParameter = queryParser.parseQuery(queryString);

		List<Restriction> restrictions = queryParameter.getRestrictions();
		assertNotNull(
				"testGetFieldsAndRestrictionsFailure() : Hint: extract the conditions from the query string(if exists). for each condition, we need to capture the following: 1. Name of field, 2. condition, 3. value, please note the query might contain multiple conditions separated by OR/AND operators",
				restrictions);

		display(queryString, queryParameter);
	}

	@Test
	public void testGetRestrictionsAndAggregateFunctions() {
		boolean aggregatestatus = false;

		queryString = "select count(city),sum(win_by_runs),min(season),max(win_by_wickets) from data/ipl.csv where season > 2014 and city ='Bangalore'";
		queryParameter = queryParser.parseQuery(queryString);

		List<Restriction> restrictions = queryParameter.getRestrictions();
		List<AggregateFunction> aggfunction = queryParameter.getAggregateFunctions();

		assertNotNull("testGetRestrictionsAndAggregateFunctions() : Empty Restrictions list", restrictions);
		assertNotNull("testGetRestrictionsAndAggregateFunctions() : Empty Aggregates list", aggfunction);

		List<String> fields = new ArrayList<String>();
		fields.add("count(city)");
		fields.add("sum(win_by_runs)");
		fields.add("min(season)");
		fields.add("max(win_by_wickets)");

		List<String> logicalop = new ArrayList<String>();
		logicalop.add("and");

		assertEquals(
				"testGetRestrictionsAndAggregateFunctions() : File name extraction failed. Check getFile() method. File name can be found after a space after from clause. Note: CSV file can contain a field that contains from as a part of the column name. For eg: from_date,from_hrs etc",
				"data/ipl.csv", queryParameter.getFile());
		assertEquals(
				"testGetRestrictionsAndAggregateFunctions() : Select fields extractions failed. The query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				fields, queryParameter.getFields());
		assertEquals(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of Base Query failed. BaseQuery contains from the beginning of the query till the where clause",
				"select count(city),sum(win_by_runs),min(season),max(win_by_wickets) from data/ipl.csv",
				queryParameter.getBaseQuery());

		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getPropertyName().contains("season"));
		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getPropertyValue().contains("2014"));
		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getCondition().contains(">"));
		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getPropertyName().contains("city"));
		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getPropertyValue().contains("Bangalore"));
		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getCondition().contains("="));

		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of Aggregate part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				aggfunction.get(0).getFunction().contains("count"));
		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of Aggregate part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				aggfunction.get(1).getFunction().contains("sum"));
		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of Aggregate part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				aggfunction.get(2).getFunction().contains("min"));
		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of Aggregate part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				aggfunction.get(3).getFunction().contains("max"));

		assertEquals(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of Logical Operators failed. AND/OR keyword will exist in the query only if where conditions exists and it contains multiple conditions.The extracted logical operators will be stored in a String array which will be returned by the method. Please note that AND/OR can exist as a substring in the conditions as well. For eg: name='Alexander',color='Red' etc.",
				logicalop, queryParameter.getLogicalOperators());

		display(queryString, queryParameter);
	}

	@Test
	public void testGetGroupByOrderByClause() {

		queryString = "select city,winner,player_of_match from data/ipl.csv where season > 2014 and city='Bangalore' group by winner order by city";
		queryParameter = queryParser.parseQuery(queryString);

		List<Restriction> restrictions = queryParameter.getRestrictions();

		List<String> fields = new ArrayList<String>();
		fields.add("city");
		fields.add("winner");
		fields.add("player_of_match");

		List<String> logicalop = new ArrayList<String>();
		logicalop.add("and");

		List<String> orderByFields = new ArrayList<String>();
		orderByFields.add("city");

		List<String> groupByFields = new ArrayList<String>();
		groupByFields.add("winner");

		assertEquals(
				"testGetGroupByOrderByClause() : File name extraction failed. Check getFile() method. File name can be found after a space after from clause. Note: CSV file can contain a field that contains from as a part of the column name. For eg: from_date,from_hrs etc",
				"data/ipl.csv", queryParameter.getFile());
		assertEquals(
				"testGetGroupByOrderByClause() : Select fields extractions failed. The query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				fields, queryParameter.getFields());
		assertEquals(
				"testGetGroupByOrderByClause() : Retrieval of Base Query failed. BaseQuery contains from the beginning of the query till the where clause",
				"select city,winner,player_of_match from data/ipl.csv", queryParameter.getBaseQuery());

		assertTrue(
				"testGetGroupByOrderByClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getPropertyName().contains("season"));
		assertTrue(
				"testGetGroupByOrderByClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getPropertyValue().contains("2014"));
		assertTrue(
				"testGetGroupByOrderByClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getCondition().contains(">"));
		assertTrue(
				"testGetGroupByOrderByClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getPropertyName().contains("city"));
		assertTrue(
				"testGetGroupByOrderByClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getPropertyValue().contains("Bangalore"));
		assertTrue(
				"testGetGroupByOrderByClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getCondition().contains("="));

		assertEquals(
				"testGetGroupByOrderByClause() : Retrieval of Logical Operators failed. AND/OR keyword will exist in the query only if where conditions exists and it contains multiple conditions.The extracted logical operators will be stored in a String array which will be returned by the method. Please note that AND/OR can exist as a substring in the conditions as well. For eg: name='Alexander',color='Red' etc.",
				logicalop, queryParameter.getLogicalOperators());

		assertEquals(
				"testGetGroupByOrderByClause() : Hint: Check getGroupByFields() method. The query string can contain more than one group by fields. it is also possible thant the query string might not contain group by clause at all. The field names, condition values might contain 'group' as a substring. For eg: newsgroup_name",
				groupByFields, queryParameter.getGroupByFields());

		assertEquals(
				"testGetGroupByOrderByClause() : Hint: Please note that we will need to extract the field(s) after 'order by' clause in the query, if at all the order by clause exists",
				orderByFields, queryParameter.getOrderByFields());

		display(queryString, queryParameter);

	}

	@Test
	public void testGetGroupByOrderByClauseFailure() {
		queryString = "select city,winner,player_of_match from data/ipl.csv where season > 2014 and city='Bangalore' group by winner order by city";
		queryParameter = queryParser.parseQuery(queryString);

		List<Restriction> restrictions = queryParameter.getRestrictions();
		assertNotNull(
				"testGetGroupByOrderByClauseFailure() : Hint: Check getGroupByFields() method. The query string can contain more than one group by fields. it is also possible thant the query string might not contain group by clause at all. The field names, condition values might contain 'group' as a substring. For eg: newsgroup_name",
				queryParameter.getGroupByFields());
		assertNotNull(
				"testGetGroupByOrderByClauseFailure() : Hint: Please note that we will need to extract the field(s) after 'order by' clause in the query, if at all the order by clause exists.",
				queryParameter.getOrderByFields());
		assertNotNull(
				"testGetGroupByOrderByClauseFailure() : Hint: extract the conditions from the query string(if exists). for each condition, we need to capture the following: 1. Name of field, 2. condition, 3. value, please note the query might contain multiple conditions separated by OR/AND operators",
				restrictions);

		display(queryString, queryParameter);
	}

	@Test
	public void testGetGroupByClause() {
		queryString = "select city,winner,player_of_match from data/ipl.csv group by city";
		queryParameter = queryParser.parseQuery(queryString);

		List<String> fields = new ArrayList<String>();
		fields.add("city");

		assertEquals(
				"testGetGroupByClause() : Hint: Check getGroupByFields() method. The query string can contain more than one group by fields. it is also possible thant the query string might not contain group by clause at all. The field names, condition values might contain 'group' as a substring. For eg: newsgroup_name",
				fields, queryParameter.getGroupByFields());
		assertNotNull(
				"testGetGroupByClause() : Hint: Check getGroupByFields() method. The query string can contain more than one group by fields. it is also possible thant the query string might not contain group by clause at all. The field names, condition values might contain 'group' as a substring. For eg: newsgroup_name",
				queryParameter.getGroupByFields());

		fields.add("winner");
		fields.add("player_of_match");

		assertEquals(
				"testGetGroupByClause() : Select fields extractions failed. The query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				fields, queryParameter.getFields());
		assertEquals(
				"testGetGroupByClause() : Retrieval of Base Query failed. BaseQuery contains from the beginning of the query till the where clause",
				"select city,winner,player_of_match from data/ipl.csv", queryParameter.getBaseQuery());
		assertEquals(
				"testGetGroupByClause() : Retrieval of conditions part is not returning null. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string",
				null, queryParameter.getRestrictions());
		assertEquals(
				"testGetGroupByClause() : Logical Operators should be null. AND/OR keyword will exist in the query only if where conditions exists and it contains multiple conditions.The extracted logical operators will be stored in a String array which will be returned by the method. Please note that AND/OR can exist as a substring in the conditions as well. For eg: name='Alexander',color='Red' etc",
				null, queryParameter.getLogicalOperators());

		display(queryString, queryParameter);

	}

	@Test
	public void testGetOrderByAndWhereConditionClause() {

		queryString = "select city,winner,player_of_match from data/ipl.csv where season > 2014 and city ='Bangalore' order by city";
		queryParameter = queryParser.parseQuery(queryString);

		List<Restriction> restrictions = queryParameter.getRestrictions();

		List<String> fields = new ArrayList<String>();
		fields.add("city");
		fields.add("winner");
		fields.add("player_of_match");

		List<String> logicalop = new ArrayList<String>();
		logicalop.add("and");

		List<String> orderByFields = new ArrayList<String>();
		orderByFields.add("city");

		assertEquals(
				"testGetOrderByAndWhereConditionClause() : File name extraction failed. Check getFile() method. File name can be found after a space after from clause. Note: CSV file can contain a field that contains from as a part of the column name. For eg: from_date,from_hrs etc",
				"data/ipl.csv", queryParameter.getFile());
		assertEquals(
				"testGetOrderByAndWhereConditionClause() : Select fields extractions failed. The query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				fields, queryParameter.getFields());
		assertEquals(
				"testGetOrderByAndWhereConditionClause() : Retrieval of Base Query failed. BaseQuery contains from the beginning of the query till the where clause",
				"select city,winner,player_of_match from data/ipl.csv", queryParameter.getBaseQuery());

		assertTrue(
				"testGetOrderByAndWhereConditionClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getPropertyName().contains("season"));

		assertTrue(
				"testGetOrderByAndWhereConditionClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getPropertyValue().contains("2014"));

		assertTrue(
				"testGetOrderByAndWhereConditionClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getCondition().contains(">"));

		assertTrue(
				"testGetOrderByAndWhereConditionClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getPropertyName().contains("city"));
		assertTrue(
				"testGetOrderByAndWhereConditionClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getPropertyValue().contains("Bangalore"));
		assertTrue(
				"testGetOrderByAndWhereConditionClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getCondition().contains("="));

		assertEquals(
				"testGetOrderByAndWhereConditionClause() : Retrieval of Logical Operators failed. AND/OR keyword will exist in the query only if where conditions exists and it contains multiple conditions.The extracted logical operators will be stored in a String array which will be returned by the method. Please note that AND/OR can exist as a substring in the conditions as well. For eg: name='Alexander',color='Red' etc.",
				logicalop, queryParameter.getLogicalOperators());

		assertEquals(
				"testGetOrderByAndWhereConditionClause() : Hint: Please note that we will need to extract the field(s) after 'order by' clause in the query, if at all the order by clause exists",
				orderByFields, queryParameter.getOrderByFields());

		display(queryString, queryParameter);

	}

	@Test
	public void testGetOrderByAndWhereConditionClauseFailure() {
		queryString = "select city,winner,player_of_match from data/ipl.csv where season > 2014 and city ='Bangalore' order by city";
		queryParameter = queryParser.parseQuery(queryString);
		List<Restriction> restrictions = queryParameter.getRestrictions();
		assertNotNull(
				"testGetOrderByAndWhereConditionClauseFailure() : Hint: extract the conditions from the query string(if exists). for each condition, we need to capture the following: 1. Name of field, 2. condition, 3. value, please note the query might contain multiple conditions separated by OR/AND operators",
				restrictions);
		assertNotNull(
				"testGetOrderByAndWhereConditionClauseFailure() :Hint: Please note that we will need to extract the field(s) after 'order by' clause in the query, if at all the order by clause exists.",
				queryParameter.getOrderByFields());
		display(queryString, queryParameter);
	}

	@Test
	public void testGetOrderByClause() {
		queryString = "select city,winner,player_of_match from data/ipl.csv where city='Bangalore' order by winner";
		queryParameter = queryParser.parseQuery(queryString);

		List<String> orderByFields = new ArrayList<String>();
		orderByFields.add("winner");

		List<String> fields = new ArrayList<String>();
		fields.add("city");
		fields.add("winner");
		fields.add("player_of_match");

		assertEquals(
				"testGetOrderByClause() : File name extraction failed. Check getFile() method. File name can be found after a space after from clause. Note: CSV file can contain a field that contains from as a part of the column name. For eg: from_date,from_hrs etc",
				"data/ipl.csv", queryParameter.getFile());
		assertEquals(
				"testGetOrderByClause() : Select fields extractions failed. The query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				fields, queryParameter.getFields());
		assertEquals(
				"testGetOrderByClause() : Hint: Please note that we will need to extract the field(s) after 'order by' clause in the query, if at all the order by clause exists",
				orderByFields, queryParameter.getOrderByFields());
		display(queryString, queryParameter);
	}

	@Test
	public void testGetOrderByWithoutWhereClause() {
		queryString = "select city,winner,player_of_match from data/ipl.csv order by city";
		queryParameter = queryParser.parseQuery(queryString);

		List<String> orderByFields = new ArrayList<String>();
		orderByFields.add("city");

		List<String> fields = new ArrayList<String>();
		fields.add("city");
		fields.add("winner");
		fields.add("player_of_match");

		assertEquals(
				"testGetOrderByWithoutWhereClause() : File name extraction failed. Check getFile() method. File name can be found after a space after from clause. Note: CSV file can contain a field that contains from as a part of the column name. For eg: from_date,from_hrs etc",
				"data/ipl.csv", queryParameter.getFile());
		assertEquals(
				"testGetOrderByWithoutWhereClause() : Select fields extractions failed. The query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				fields, queryParameter.getFields());
		assertEquals(
				"testGetOrderByWithoutWhereClause() : Hint: Please note that we will need to extract the field(s) after 'order by' clause in the query, if at all the order by clause exists",
				orderByFields, queryParameter.getOrderByFields());

		display(queryString, queryParameter);

	}

	private void display(String queryString, QueryParameter queryParameter) {
		System.out.println("\nQuery : " + queryString);
		System.out.println("--------------------------------------------------");
		System.out.println("Base Query:" + queryParameter.getBaseQuery());
		System.out.println("File:" + queryParameter.getFile());
		System.out.println("Query Type:" + queryParameter.getQUERY_TYPE());
		List<String> fields = queryParameter.getFields();
		System.out.println("Selected field(s):");
		if (fields == null || fields.isEmpty()) {
			System.out.println("*");
		} else {
			for (String field : fields) {
				System.out.println("\t" + field);
			}
		}

		List<Restriction> restrictions = queryParameter.getRestrictions();

		if (restrictions != null && !restrictions.isEmpty()) {
			System.out.println("Where Conditions : ");
			int conditionCount = 1;
			for (Restriction restriction : restrictions) {
				System.out.println("\tCondition : " + conditionCount++);
				System.out.println("\t\tName : " + restriction.getPropertyName());
				System.out.println("\t\tCondition : " + restriction.getCondition());
				System.out.println("\t\tValue : " + restriction.getPropertyValue());
			}
		}
		List<AggregateFunction> aggregateFunctions = queryParameter.getAggregateFunctions();
		if (aggregateFunctions != null && !aggregateFunctions.isEmpty()) {

			System.out.println("Aggregate Functions : ");
			int funtionCount = 1;
			for (AggregateFunction aggregateFunction : aggregateFunctions) {
				System.out.println("\t Aggregate Function : " + funtionCount++);
				System.out.println("\t\t function : " + aggregateFunction.getFunction());
				System.out.println("\t\t  field : " + aggregateFunction.getField());
			}

		}

		List<String> orderByFields = queryParameter.getOrderByFields();
		if (orderByFields != null && !orderByFields.isEmpty()) {

			System.out.println(" Order by fields : ");
			for (String orderByField : orderByFields) {
				System.out.println("\t " + orderByField);

			}

		}

		List<String> groupByFields = queryParameter.getGroupByFields();
		if (groupByFields != null && !groupByFields.isEmpty()) {

			System.out.println(" Group by fields : ");
			for (String groupByField : groupByFields) {
				System.out.println("\t " + groupByField);

			}

		}

	}

	/*
	 * The following test cases are used to check whether the query processing are
	 * working properly
	 */

	@Test
	public void testSelectAllWithoutWhereClause() throws FileNotFoundException {

		int totalRecordsExpected = 577;

		HashMap dataSet = query.executeQuery("select * from data/ipl.csv");

		boolean totalColumnsExpected = dataSet.entrySet().iterator().next().toString().split(",").length == 18;

		assertNotNull("testSelectAllWithoutWhereClause() : Empty Dataset returned", dataSet);
		assertEquals("testSelectAllWithoutWhereClause() : Total number of records should be 577", totalRecordsExpected,
				dataSet.size());
		assertEquals("testSelectAllWithoutWhereClause() : Total number of columns should be 18", true,
				totalColumnsExpected);

		assertTrue(
				"testSelectAllWithoutWhereClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"1={date=2008-04-18, venue=M Chinnaswamy Stadium, win_by_wickets=0, city=Bangalore, team1=Kolkata Knight Riders, team2=Royal Challengers Bangalore, result=normal, dl_applied=0, winner=Kolkata Knight Riders, player_of_match=BB McCullum, umpire1=Asad Rauf, season=2008, toss_winner=Royal Challengers Bangalore, umpire3=, id=1, umpire2=RE Koertzen, toss_decision=field, win_by_runs=140}"));

		assertTrue(
				"testSelectAllWithoutWhereClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"289={date=2012-05-01, venue=Barabati Stadium, win_by_wickets=0, city=Cuttack, team1=Deccan Chargers, team2=Pune Warriors, result=normal, dl_applied=0, winner=Deccan Chargers, player_of_match=KC Sangakkara, umpire1=Aleem Dar, season=2012, toss_winner=Deccan Chargers, umpire3=, id=289, umpire2=AK Chaudhary, toss_decision=bat, win_by_runs=13}"));
		assertTrue(
				"testSelectAllWithoutWhereClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"577={date=2016-05-29, venue=M Chinnaswamy Stadium, win_by_wickets=0, city=Bangalore, team1=Sunrisers Hyderabad, team2=Royal Challengers Bangalore, result=normal, dl_applied=0, winner=Sunrisers Hyderabad, player_of_match=BCJ Cutting, umpire1=HDPK Dharmasena, season=2016, toss_winner=Sunrisers Hyderabad, umpire3=, id=577, umpire2=BNJ Oxenford, toss_decision=bat, win_by_runs=8}"));

		assertTrue("testSelectAllWithoutWhereClause() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testSelectAllWithoutWhereClause() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));
		display("testSelectAllWithoutWhereClause", dataSet);

	}

	@Test
	public void testSelectColumnsWithoutWhereClause() throws FileNotFoundException {
		int totalRecordsExpected = 577;

		HashMap dataSet = query.executeQuery("select winner,city,team1,team2 from data/ipl.csv");

		boolean totalColumnsExpected = dataSet.entrySet().iterator().next().toString().split(",").length == 4;

		assertNotNull("testSelectColumnsWithoutWhereClause() : Empty Dataset returned", dataSet);
		assertEquals("testSelectColumnsWithoutWhereClause() : Total number of records should be 577",
				totalRecordsExpected, dataSet.size());
		assertEquals("testSelectColumnsWithoutWhereClause() : Total number of columns should be 4", true,
				totalColumnsExpected);

		assertTrue(
				"testSelectColumnsWithoutWhereClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"1={winner=Kolkata Knight Riders, city=Bangalore, team1=Kolkata Knight Riders, team2=Royal Challengers Bangalore}"));

		assertTrue(
				"testSelectColumnsWithoutWhereClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"289={winner=Deccan Chargers, city=Cuttack, team1=Deccan Chargers, team2=Pune Warriors}"));

		assertTrue(
				"testSelectColumnsWithoutWhereClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"577={winner=Sunrisers Hyderabad, city=Bangalore, team1=Sunrisers Hyderabad, team2=Royal Challengers Bangalore}"));

		assertTrue("testSelectColumnsWithoutWhereClause() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testSelectColumnsWithoutWhereClause() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testSelectColumnsWithoutWhereClause", dataSet);

	}

	@Test
	public void testWithWhereGreaterThanClause() throws FileNotFoundException {
		int totalRecordsExpected = 60;

		HashMap dataSet = query.executeQuery(
				"select winner,player_of_match,city,team1,team2,season from data/ipl.csv where season > 2015");

		boolean totalColumnsExpected = dataSet.entrySet().iterator().next().toString().split(",").length == 6;

		assertNotNull("testWithWhereGreaterThanClause() : Empty Dataset returned", dataSet);
		assertEquals("testWithWhereGreaterThanClause() : Total number of records should be 60", totalRecordsExpected,
				dataSet.size());
		assertEquals("testWithWhereGreaterThanClause() : Total number of columns should be 6", true,
				totalColumnsExpected);

		assertTrue(
				"testWithWhereGreaterThanClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"1={winner=Rising Pune Supergiants, player_of_match=AM Rahane, city=Mumbai, team1=Mumbai Indians, team2=Rising Pune Supergiants, season=2016}"));

		assertTrue(
				"testWithWhereGreaterThanClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"30={winner=Kolkata Knight Riders, player_of_match=AD Russell, city=Bangalore, team1=Royal Challengers Bangalore, team2=Kolkata Knight Riders, season=2016}"));

		assertTrue(
				"testWithWhereGreaterThanClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"60={winner=Sunrisers Hyderabad, player_of_match=BCJ Cutting, city=Bangalore, team1=Sunrisers Hyderabad, team2=Royal Challengers Bangalore, season=2016}"));

		assertTrue("testWithWhereGreaterThanClause() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testWithWhereGreaterThanClause() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testWithWhereGreaterThanClause", dataSet);

	}

	@Test
	public void testWithWhereLessThanClause() throws FileNotFoundException {
		int totalRecordsExpected = 458;

		HashMap dataSet = query
				.executeQuery("select winner,player_of_match,city,team1,team2 from data/ipl.csv where season < 2015");

		boolean totalColumnsExpected = dataSet.entrySet().iterator().next().toString().split(",").length == 5;

		assertNotNull("testWithWhereLessThanClause() : Empty Dataset returned", dataSet);
		assertEquals("testWithWhereLessThanClause() : Total number of records should be 458", totalRecordsExpected,
				dataSet.size());
		assertEquals("testWithWhereLessThanClause() : Total number of columns should be 5", true, totalColumnsExpected);

		assertTrue(
				"testWithWhereLessThanClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"1={winner=Kolkata Knight Riders, player_of_match=BB McCullum, city=Bangalore, team1=Kolkata Knight Riders, team2=Royal Challengers Bangalore}"));

		assertTrue(
				"testWithWhereLessThanClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"229={winner=Royal Challengers Bangalore, player_of_match=S Aravind, city=Jaipur, team1=Rajasthan Royals, team2=Royal Challengers Bangalore}"));

		assertTrue(
				"testWithWhereLessThanClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"458={winner=Kolkata Knight Riders, player_of_match=MK Pandey, city=Bangalore, team1=Kings XI Punjab, team2=Kolkata Knight Riders}"));

		assertTrue("testWithWhereLessThanClause() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testWithWhereLessThanClause() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testWithWhereLessThanClause", dataSet);

	}

	@Test
	public void testWithWhereLessThanOrEqualToClause() throws FileNotFoundException {
		int totalRecordsExpected = 517;

		HashMap dataSet = query.executeQuery(
				"select winner,player_of_match,city,team1,team2,season from data/ipl.csv where season <= 2015");

		boolean totalColumnsExpected = dataSet.entrySet().iterator().next().toString().split(",").length == 6;

		assertNotNull("testWithWhereLessThanOrEqualToClause() : Empty Dataset returned", dataSet);
		assertEquals("testWithWhereLessThanOrEqualToClause() : Total number of records should be 517",
				totalRecordsExpected, dataSet.size());
		assertEquals("testWithWhereLessThanOrEqualToClause() : Total number of columns should be 6", true,
				totalColumnsExpected);

		assertTrue(
				"testWithWhereLessThanOrEqualToClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"1={winner=Kolkata Knight Riders, player_of_match=BB McCullum, city=Bangalore, team1=Kolkata Knight Riders, team2=Royal Challengers Bangalore, season=2008}"));

		assertTrue(
				"testWithWhereLessThanOrEqualToClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"258={winner=Kolkata Knight Riders, player_of_match=L Balaji, city=Bangalore, team1=Kolkata Knight Riders, team2=Royal Challengers Bangalore, season=2012}"));
		assertTrue(
				"testWithWhereLessThanOrEqualToClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"517={winner=Mumbai Indians, player_of_match=RG Sharma, city=Kolkata, team1=Mumbai Indians, team2=Chennai Super Kings, season=2015}"));

		assertTrue("testWithWhereLessThanOrEqualToClause() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testWithWhereLessThanOrEqualToClause() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testWithWhereLessThanOrEqualToClause", dataSet);

	}

	@Test
	public void testWithWhereGreaterThanOrEqualToClause() throws FileNotFoundException {
		int totalRecordsExpected = 119;

		HashMap dataSet = query
				.executeQuery("select winner,player_of_match,city,team1,team2 from data/ipl.csv where season >= 2015");

		boolean totalColumnsExpected = dataSet.entrySet().iterator().next().toString().split(",").length == 5;

		assertNotNull("testWithWhereGreaterThanOrEqualToClause() : Empty Dataset returned", dataSet);
		assertEquals("testWithWhereGreaterThanOrEqualToClause() : Total number of records should be 119",
				totalRecordsExpected, dataSet.size());
		assertEquals("testWithWhereGreaterThanOrEqualToClause() : Total number of columns should be 5", true,
				totalColumnsExpected);

		assertTrue(
				"testWithWhereGreaterThanOrEqualToClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"1={winner=Kolkata Knight Riders, player_of_match=M Morkel, city=Kolkata, team1=Mumbai Indians, team2=Kolkata Knight Riders}"));

		assertTrue(
				"testWithWhereGreaterThanOrEqualToClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"60={winner=Rising Pune Supergiants, player_of_match=AM Rahane, city=Mumbai, team1=Mumbai Indians, team2=Rising Pune Supergiants}"));

		assertTrue(
				"testWithWhereGreaterThanOrEqualToClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"119={winner=Sunrisers Hyderabad, player_of_match=BCJ Cutting, city=Bangalore, team1=Sunrisers Hyderabad, team2=Royal Challengers Bangalore}"));

		assertTrue("testWithWhereGreaterThanOrEqualToClause() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testWithWhereGreaterThanOrEqualToClause() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testWithWhereGreaterThanOrEqualToClause", dataSet);

	}

	@Test
	public void testWithWhereNotEqualToClause() throws FileNotFoundException {

		int totalRecordsExpected = 315;

		HashMap dataSet = query.executeQuery(
				"select winner,city,team1,team2,toss_decision from data/ipl.csv where toss_decision != bat");

		boolean totalColumnsExpected = dataSet.entrySet().iterator().next().toString().split(",").length == 5;

		assertNotNull("testWithWhereNotEqualToClause() : Empty Dataset returned", dataSet);
		assertEquals("testWithWhereNotEqualToClause() : Total number of records should be 315", totalRecordsExpected,
				dataSet.size());
		assertEquals("testWithWhereNotEqualToClause() : Total number of columns should be 5", true,
				totalColumnsExpected);

		assertTrue(
				"testWithWhereNotEqualToClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"1={winner=Kolkata Knight Riders, city=Bangalore, team1=Kolkata Knight Riders, team2=Royal Challengers Bangalore, toss_decision=field}"));

		assertTrue(
				"testWithWhereNotEqualToClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"157={winner=Delhi Daredevils, city=Dharamsala, team1=Kings XI Punjab, team2=Delhi Daredevils, toss_decision=field}"));

		assertTrue(
				"testWithWhereNotEqualToClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"315={winner=Sunrisers Hyderabad, city=Delhi, team1=Gujarat Lions, team2=Sunrisers Hyderabad, toss_decision=field}"));

		assertTrue("testWithWhereNotEqualToClause() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testWithWhereNotEqualToClause() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testWithWhereNotEqualToClause", dataSet);

	}

	@Test
	public void testWithWhereEqualAndNotEqualClause() throws FileNotFoundException {
		int totalRecordsExpected = 195;

		HashMap dataSet = query.executeQuery(
				"select winner,player_of_match,city,team1,team2,season from data/ipl.csv where season >= 2013 and season <= 2015");

		boolean totalColumnsExpected = dataSet.entrySet().iterator().next().toString().split(",").length == 6;

		assertNotNull("testWithWhereEqualAndNotEqualClause() : Empty Dataset returned", dataSet);
		assertEquals("testWithWhereEqualAndNotEqualClause() : Total number of records should be 195",
				totalRecordsExpected, dataSet.size());
		assertEquals("testWithWhereEqualAndNotEqualClause() : Total number of columns should be 6", true,
				totalColumnsExpected);

		assertTrue(
				"testWithWhereEqualAndNotEqualClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"1={winner=Kolkata Knight Riders, player_of_match=SP Narine, city=Kolkata, team1=Delhi Daredevils, team2=Kolkata Knight Riders, season=2013}"));
		assertTrue(
				"testWithWhereEqualAndNotEqualClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"97={winner=Chennai Super Kings, player_of_match=RA Jadeja, city=Ranchi, team1=Chennai Super Kings, team2=Kolkata Knight Riders, season=2014}"));
		assertTrue(
				"testWithWhereEqualAndNotEqualClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"195={winner=Mumbai Indians, player_of_match=RG Sharma, city=Kolkata, team1=Mumbai Indians, team2=Chennai Super Kings, season=2015}"));

		assertTrue("testWithWhereEqualAndNotEqualClause() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testWithWhereEqualAndNotEqualClause() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testWithWhereEqualAndNotEqualClause", dataSet);

	}

	@Test
	public void testWithWhereTwoConditionsEqualOrNotEqualClause() throws FileNotFoundException {
		int totalRecordsExpected = 155;

		HashMap dataSet = query.executeQuery(
				"select winner,player_of_match,city,team1,team2 from data/ipl.csv where season >= 2013 and toss_decision != bat");

		boolean totalColumnsExpected = dataSet.entrySet().iterator().next().toString().split(",").length == 5;

		assertNotNull("testWithWhereTwoConditionsEqualOrNotEqualClause() : Empty Dataset returned", dataSet);
		assertEquals("testWithWhereTwoConditionsEqualOrNotEqualClause() : Total number of records should be 155",
				totalRecordsExpected, dataSet.size());
		assertEquals("testWithWhereTwoConditionsEqualOrNotEqualClause() : Total number of columns should be 5", true,
				totalColumnsExpected);

		assertTrue(
				"testWithWhereTwoConditionsEqualOrNotEqualClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"1={winner=Kolkata Knight Riders, player_of_match=SP Narine, city=Kolkata, team1=Delhi Daredevils, team2=Kolkata Knight Riders}"));

		assertTrue(
				"testWithWhereTwoConditionsEqualOrNotEqualClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"78={winner=Kings XI Punjab, player_of_match=GJ Bailey, city=Mumbai, team1=Kings XI Punjab, team2=Mumbai Indians}"));

		assertTrue(
				"testWithWhereTwoConditionsEqualOrNotEqualClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"155={winner=Sunrisers Hyderabad, player_of_match=DA Warner, city=Delhi, team1=Gujarat Lions, team2=Sunrisers Hyderabad}"));

		assertTrue("testWithWhereTwoConditionsEqualOrNotEqualClause(): Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testWithWhereTwoConditionsEqualOrNotEqualClause() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testWithWhereTwoConditionsEqualOrNotEqualClause", dataSet);

	}

	@Test
	public void testWithWhereThreeConditionsEqualOrNotEqualClause() throws FileNotFoundException {
		int totalRecordsExpected = 577;

		HashMap dataSet = query.executeQuery(
				"select winner,player_of_match,city,team1,team2 from data/ipl.csv where season >= 2008 or toss_decision != bat and city = bangalore");

		boolean totalColumnsExpected = dataSet.entrySet().iterator().next().toString().split(",").length == 5;

		assertNotNull("testWithWhereThreeConditionsEqualOrNotEqualClause() : Empty Dataset returned", dataSet);
		assertEquals("testWithWhereThreeConditionsEqualOrNotEqualClause() : Total number of records should be 577",
				totalRecordsExpected, dataSet.size());
		assertEquals("testWithWhereThreeConditionsEqualOrNotEqualClause() : Total number of columns should be 5", true,
				totalColumnsExpected);
		assertTrue(
				"testWithWhereThreeConditionsEqualOrNotEqualClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"1={winner=Kolkata Knight Riders, player_of_match=BB McCullum, city=Bangalore, team1=Kolkata Knight Riders, team2=Royal Challengers Bangalore}"));

		assertTrue(
				"testWithWhereThreeConditionsEqualOrNotEqualClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"289={winner=Deccan Chargers, player_of_match=KC Sangakkara, city=Cuttack, team1=Deccan Chargers, team2=Pune Warriors}"));

		assertTrue(
				"testWithWhereThreeConditionsEqualOrNotEqualClause() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"577={winner=Sunrisers Hyderabad, player_of_match=BCJ Cutting, city=Bangalore, team1=Sunrisers Hyderabad, team2=Royal Challengers Bangalore}"));

		assertTrue("testWithWhereThreeConditionsEqualOrNotEqualClause() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testWithWhereThreeConditionsEqualOrNotEqualClause() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testWithWhereThreeConditionsEqualOrNotEqualClause", dataSet);

	}

	@Test
	public void testWithWhereThreeConditionsOrderBy() throws FileNotFoundException {

		int totalRecordsExpected = 278;

		HashMap dataSet = query.executeQuery(
				"select winner,player_of_match,city,team1,team2 from data/ipl.csv where season >= 2013 or toss_decision != bat and city = Bangalore order by winner");

		boolean totalColumnsExpected = dataSet.entrySet().iterator().next().toString().split(",").length == 5;

		assertNotNull("testWithWhereThreeConditionsOrderBy() : Empty Dataset returned", dataSet);
		assertEquals("testWithWhereThreeConditionsOrderBy() : Total number of records should be 278",
				totalRecordsExpected, dataSet.size());
		assertEquals("testWithWhereThreeConditionsOrderBy() : Total number of columns should be 5", true,
				totalColumnsExpected);

		assertTrue(
				"testWithWhereThreeConditionsOrderBy() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"1={winner=, player_of_match=, city=Bangalore, team1=Royal Challengers Bangalore, team2=Rajasthan Royals}"));
		assertTrue(
				"testWithWhereThreeConditionsOrderBy() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"139={winner=Mumbai Indians, player_of_match=SR Tendulkar, city=Mumbai, team1=Mumbai Indians, team2=Kolkata Knight Riders}"));
		assertTrue(
				"testWithWhereThreeConditionsOrderBy() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"278={winner=Sunrisers Hyderabad, player_of_match=BCJ Cutting, city=Bangalore, team1=Sunrisers Hyderabad, team2=Royal Challengers Bangalore}"));

		assertTrue("testWithWhereThreeConditionsOrderBy() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testWithWhereThreeConditionsOrderBy() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testWithWhereThreeConditionsOrderBy", dataSet);

	}

	@Test
	public void testWithWhereThreeConditionsGroupBy() throws FileNotFoundException {

		HashMap dataSet = query.executeQuery(
				"select city,winner,team1,team2,player_of_match from data/ipl.csv where season >= 2013 or toss_decision != bat and city = Bangalore group by team1");

		boolean totalColumnsExpected = dataSet.entrySet().iterator().next().toString().split(",").length == 30;
		boolean dataExpectedStatus = (dataSet.toString().contains(
				"{Kolkata Knight Riders={1={winner=Kolkata Knight Riders, player_of_match=BB McCullum, city=Bangalore, team1=Kolkata Knight Riders, team2=Royal Challengers Bangalore}, 158={winner=Royal Challengers Bangalore, player_of_match=R Vinay Kumar, city=Bangalore, team1=Kolkata Knight Riders, team2=Royal Challengers Bangalore}, 232={winner=Royal Challengers Bangalore, player_of_match=CH Gayle, city=Bangalore, team1=Kolkata Knight Riders, team2=Royal Challengers Bangalore}, 258={winner=Kolkata Knight Riders, player_of_match=L Balaji, city=Bangalore, team1=Kolkata Knight Riders, team2=Royal Challengers Bangalore}, 333={winner=Royal Challengers Bangalore, player_of_match=CH Gayle, city=Bangalore, team1=Kolkata Knight Riders, team2=Royal Challengers Bangalore}, 491={winner=Royal Challengers Bangalore, player_of_match=Mandeep Singh, city=Bangalore, team1=Kolkata Knight Riders, team2=Royal Challengers Bangalore}}, Royal Challengers Bangalore={11={winner=Rajasthan Royals, player_of_match=SR Watson, city=Bangalore, team1=Royal Challengers Bangalore, team2=Rajasthan Royals}, 25={winner=Kings XI Punjab, player_of_match=S Sreesanth, city=Bangalore, team1=Royal Challengers Bangalore, team2=Kings XI Punjab}, 31={winner=Mumbai Indians, player_of_match=CRD Fernando, city=Bangalore, team1=Royal Challengers Bangalore, team2=Mumbai Indians}, 45={winner=Delhi Daredevils, player_of_match=SP Goswami, city=Bangalore, team1=Royal Challengers Bangalore, team2=Delhi Daredevils}, 52={winner=Royal Challengers Bangalore, player_of_match=P Kumar, city=Bangalore, team1=Royal Challengers Bangalore, team2=Deccan Chargers}, 133={winner=Royal Challengers Bangalore, player_of_match=RV Uthappa, city=Bangalore, team1=Royal Challengers Bangalore, team2=Chennai Super Kings}, 155={winner=Deccan Chargers, player_of_match=TL Suman, city=Bangalore, team1=Royal Challengers Bangalore, team2=Deccan Chargers}, 183={winner=Mumbai Indians, player_of_match=SR Tendulkar, city=Bangalore, team1=Royal Challengers Bangalore, team2=Mumbai Indians}, 209={winner=Royal Challengers Bangalore, player_of_match=V Kohli, city=Bangalore, team1=Royal Challengers Bangalore, team2=Pune Warriors}, 221={winner=Royal Challengers Bangalore, player_of_match=CH Gayle, city=Bangalore, team1=Royal Challengers Bangalore, team2=Kings XI Punjab}, 253={winner=Royal Challengers Bangalore, player_of_match=AB de Villiers, city=Bangalore, team1=Royal Challengers Bangalore, team2=Delhi Daredevils}, 291={winner=Kings XI Punjab, player_of_match=Azhar Mahmood, city=Bangalore, team1=Royal Challengers Bangalore, team2=Kings XI Punjab}, 308={winner=Mumbai Indians, player_of_match=AT Rayudu, city=Bangalore, team1=Royal Challengers Bangalore, team2=Mumbai Indians}, 324={winner=Royal Challengers Bangalore, player_of_match=CH Gayle, city=Bangalore, team1=Royal Challengers Bangalore, team2=Mumbai Indians}, 352={winner=Royal Challengers Bangalore, player_of_match=CH Gayle, city=Bangalore, team1=Royal Challengers Bangalore, team2=Pune Warriors}, 370={winner=Kings XI Punjab, player_of_match=AC Gilchrist, city=Bangalore, team1=Royal Challengers Bangalore, team2=Kings XI Punjab}, 393={winner=Royal Challengers Bangalore, player_of_match=V Kohli, city=Bangalore, team1=Royal Challengers Bangalore, team2=Chennai Super Kings}, 433={winner=Rajasthan Royals, player_of_match=JP Faulkner, city=Bangalore, team1=Royal Challengers Bangalore, team2=Rajasthan Royals}, 436={winner=Royal Challengers Bangalore, player_of_match=Yuvraj Singh, city=Bangalore, team1=Royal Challengers Bangalore, team2=Delhi Daredevils}, 451={winner=Chennai Super Kings, player_of_match=MS Dhoni, city=Bangalore, team1=Royal Challengers Bangalore, team2=Chennai Super Kings}, 466={winner=Sunrisers Hyderabad, player_of_match=DA Warner, city=Bangalore, team1=Royal Challengers Bangalore, team2=Sunrisers Hyderabad}, 487={winner=, player_of_match=, city=Bangalore, team1=Royal Challengers Bangalore, team2=Rajasthan Royals}, 498={winner=Royal Challengers Bangalore, player_of_match=CH Gayle, city=Bangalore, team1=Royal Challengers Bangalore, team2=Kings XI Punjab}, 521={winner=Royal Challengers Bangalore, player_of_match=AB de Villiers, city=Bangalore, team1=Royal Challengers Bangalore, team2=Sunrisers Hyderabad}, 528={winner=Delhi Daredevils, player_of_match=Q de Kock, city=Bangalore, team1=Royal Challengers Bangalore, team2=Delhi Daredevils}, 547={winner=Kolkata Knight Riders, player_of_match=AD Russell, city=Bangalore, team1=Royal Challengers Bangalore, team2=Kolkata Knight Riders}, 558={winner=Mumbai Indians, player_of_match=KH Pandya, city=Bangalore, team1=Royal Challengers Bangalore, team2=Mumbai Indians}, 561={winner=Royal Challengers Bangalore, player_of_match=AB de Villiers, city=Bangalore, team1=Royal Challengers Bangalore, team2=Gujarat Lions}, 567={winner=Royal Challengers Bangalore, player_of_match=V Kohli, city=Bangalore, team1=Royal Challengers Bangalore, team2=Kings XI Punjab}}, Chennai Super Kings={15={winner=Chennai Super Kings, player_of_match=MS Dhoni, city=Bangalore, team1=Chennai Super Kings, team2=Royal Challengers Bangalore}, 243={winner=Royal Challengers Bangalore, player_of_match=CH Gayle, city=Bangalore, team1=Chennai Super Kings, team2=Royal Challengers Bangalore}, 320={winner=Chennai Super Kings, player_of_match=MS Dhoni, city=Bangalore, team1=Chennai Super Kings, team2=Mumbai Indians}, 479={winner=Chennai Super Kings, player_of_match=SK Raina, city=Bangalore, team1=Chennai Super Kings, team2=Royal Challengers Bangalore}}, Kings XI Punjab={122={winner=Royal Challengers Bangalore, player_of_match=JH Kallis, city=Bangalore, team1=Kings XI Punjab, team2=Royal Challengers Bangalore}, 429={winner=Kings XI Punjab, player_of_match=Sandeep Sharma, city=Bangalore, team1=Kings XI Punjab, team2=Royal Challengers Bangalore}, 458={winner=Kolkata Knight Riders, player_of_match=MK Pandey, city=Bangalore, team1=Kings XI Punjab, team2=Kolkata Knight Riders}}, Rajasthan Royals={125={winner=Royal Challengers Bangalore, player_of_match=JH Kallis, city=Bangalore, team1=Rajasthan Royals, team2=Royal Challengers Bangalore}, 267={winner=Rajasthan Royals, player_of_match=AM Rahane, city=Bangalore, team1=Rajasthan Royals, team2=Royal Challengers Bangalore}, 348={winner=Royal Challengers Bangalore, player_of_match=R Vinay Kumar, city=Bangalore, team1=Rajasthan Royals, team2=Royal Challengers Bangalore}}, Delhi Daredevils={138={winner=Delhi Daredevils, player_of_match=KM Jadhav, city=Bangalore, team1=Delhi Daredevils, team2=Royal Challengers Bangalore}, 342={winner=Royal Challengers Bangalore, player_of_match=V Kohli, city=Bangalore, team1=Delhi Daredevils, team2=Royal Challengers Bangalore}, 512={winner=, player_of_match=, city=Bangalore, team1=Delhi Daredevils, team2=Royal Challengers Bangalore}}, Mumbai Indians={167={winner=Mumbai Indians, player_of_match=R McLaren, city=Bangalore, team1=Mumbai Indians, team2=Royal Challengers Bangalore}, 475={winner=Mumbai Indians, player_of_match=Harbhajan Singh, city=Bangalore, team1=Mumbai Indians, team2=Royal Challengers Bangalore}}, Kochi Tuskers Kerala={224={winner=Royal Challengers Bangalore, player_of_match=CH Gayle, city=Bangalore, team1=Kochi Tuskers Kerala, team2=Royal Challengers Bangalore}}, Pune Warriors={270={winner=Royal Challengers Bangalore, player_of_match=CH Gayle, city=Bangalore, team1=Pune Warriors, team2=Royal Challengers Bangalore}}, Deccan Chargers={297={winner=Royal Challengers Bangalore, player_of_match=AB de Villiers, city=Bangalore, team1=Deccan Chargers, team2=Royal Challengers Bangalore}}, Sunrisers Hyderabad={373={winner=Royal Challengers Bangalore, player_of_match=V Kohli, city=Bangalore, team1=Sunrisers Hyderabad, team2=Royal Challengers Bangalore}, 422={winner=Royal Challengers Bangalore, player_of_match=AB de Villiers, city=Bangalore, team1=Sunrisers Hyderabad, team2=Royal Challengers Bangalore}, 577={winner=Sunrisers Hyderabad, player_of_match=BCJ Cutting, city=Bangalore, team1=Sunrisers Hyderabad, team2=Royal Challengers Bangalore}}, Rising Pune Supergiants={552={winner=Royal Challengers Bangalore, player_of_match=V Kohli, city=Bangalore, team1=Rising Pune Supergiants, team2=Royal Challengers Bangalore}}, Gujarat Lions={574={winner=Royal Challengers Bangalore, player_of_match=AB de Villiers, city=Bangalore, team1=Gujarat Lions, team2=Royal Challengers Bangalore}}}"));

		assertNotNull("testWithWhereThreeConditionsGroupBy() : Empty Dataset returned", dataSet);
		assertEquals("testWithWhereThreeConditionsGroupBy() : Total number of columns should be 5", true,
				totalColumnsExpected);
		assertEquals(
				"testWithWhereThreeConditionsGroupBy() : Total number of records are matching but the records returned does not match the expected data",
				true, dataExpectedStatus);
		assertTrue("testWithWhereThreeConditionsGroupBy() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testWithWhereThreeConditionsGroupBy() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testWithWhereThreeConditionsGroupBy", dataSet);

	}

	@Test
	public void testWithOrderBy() throws FileNotFoundException {

		int totalRecordsExpected = 577;

		HashMap dataSet = query.executeQuery("select player_of_match,city,team1,team2 from data/ipl.csv order by city");

		boolean totalColumnsExpected = dataSet.entrySet().iterator().next().toString().split(",").length == 4;

		assertNotNull("testWithOrderBy() : Empty Dataset returned", dataSet);
		assertEquals("testWithOrderBy() : Total number of records should be 577", totalRecordsExpected, dataSet.size());
		assertEquals("testWithOrderBy() : Total number of columns should be 4", true, totalColumnsExpected);

		assertTrue("testWithOrderBy() : Count for the city Bangalore does not match the expected value",
				dataSet.toString().contains(
						"1={player_of_match=PA Patel, city=, team1=Mumbai Indians, team2=Royal Challengers Bangalore}"));
		assertTrue("testWithOrderBy() : Count for the city Bangalore does not match the expected value",
				dataSet.toString().contains(
						"288={player_of_match=DJ Bravo, city=Hyderabad, team1=Mumbai Indians, team2=Deccan Chargers}"));
		assertTrue(
				"testWithOrderBy() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"577={player_of_match=MS Dhoni, city=Visakhapatnam, team1=Kings XI Punjab, team2=Rising Pune Supergiants}"));

		assertTrue("testWithOrderBy() : Writing data into json format has failed", jsonWriter.writeToJson(dataSet));
		assertTrue("testWithOrderBy() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testWithOrderBy", dataSet);

	}

	@Test
	public void testSelectCountColumnsWithoutWhere() throws FileNotFoundException {

		HashMap dataSet = query.executeQuery("select count(city) from data/ipl.csv");
		boolean dataExpectedStatus = (dataSet.get("count(city)").toString().contains("570"));

		assertNotNull("testSelectCountColumnsWithoutWhere() : Invalid count returned", dataSet);
		assertEquals("testSelectCountColumnsWithoutWhere() : Total records returned should be 1", 1, dataSet.size());
		assertEquals(
				"testSelectCountColumnsWithoutWhere() : Total number of records are matching but the records returned does not match the expected data",
				true, dataExpectedStatus);
		assertTrue("testSelectCountColumnsWithoutWhere() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testSelectCountColumnsWithoutWhere() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testSelectCountColumnsWithoutWhere", dataSet);

	}

	@Test
	public void testSelectSumColumnsWithoutWhere() throws FileNotFoundException {

		HashMap dataSet = query.executeQuery("select sum(win_by_runs) from data/ipl.csv");
		boolean dataExpectedStatus = (dataSet.get("sum(win_by_runs)").toString().contains("7914"));

		assertNotNull("testSelectSumColumnsWithoutWhere() : Empty result returned", dataSet);
		assertEquals("testSelectSumColumnsWithoutWhere() : Total number of records should be 1", 1, dataSet.size());
		assertEquals("testSelectSumColumnsWithoutWhere() : sum(win_by_runs) does not match the expected result", true,
				dataExpectedStatus);
		assertTrue("testSelectSumColumnsWithoutWhere() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testSelectSumColumnsWithoutWhere() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testSelectSumColumnsWithoutWhere", dataSet);

	}

	@Test
	public void testSelectCountColumnsWithoutWhere2() throws FileNotFoundException {

		int totalRecordsExpected = 5;

		HashMap dataSet = query.executeQuery(
				"select count(city),sum(win_by_runs),min(win_by_runs),max(win_by_runs),avg(win_by_runs) from data/ipl.csv");

		assertNotNull("testSelectCountColumnsWithoutWhere2() : Empty Dataset returned", dataSet);
		assertEquals("testSelectCountColumnsWithoutWhere2() : Total number of records should be 5",
				totalRecordsExpected, dataSet.size());

		assertTrue("testSelectCountColumnsWithoutWhere2() : count(city) does not match with the expected value",
				dataSet.get("count(city)").toString().contains("570"));
		assertTrue("testSelectCountColumnsWithoutWhere2() : sum(win_by_runs) does not match with the expected value",
				dataSet.get("sum(win_by_runs)").toString().contains("7914.0"));
		assertTrue("testSelectCountColumnsWithoutWhere2() : min(win_by_runs) does not match with the expected value",
				dataSet.get("min(win_by_runs)").toString().contains("0"));
		assertTrue("testSelectCountColumnsWithoutWhere2() : max(win_by_runs) does not match with the expected value",
				dataSet.get("max(win_by_runs)").toString().contains("144"));
		assertTrue("testSelectCountColumnsWithoutWhere2() : avg(win_by_runs) does not match with the expected value",
				dataSet.get("avg(win_by_runs)").toString().contains("13.71"));

		assertTrue("testSelectCountColumnsWithoutWhere2() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testSelectCountColumnsWithoutWhere2() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testSelectCountColumnsWithoutWhere2", dataSet);

	}

	@Test
	public void testSelectColumnsWithoutWhereWithGroupByCount() throws FileNotFoundException {

		int totalRecordsExpected = 31;

		HashMap dataSet = query.executeQuery("select city,count(*) from data/ipl.csv group by city");

		assertNotNull("testSelectColumnsWithoutWhereWithGroupByCount() : Empty Dataset returned", dataSet);
		assertEquals("testSelectColumnsWithoutWhereWithGroupByCount() : Total number of records should be 31",
				totalRecordsExpected, dataSet.size());
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByCount() : Count for the city Bangalore does not match the expected value",
				dataSet.get("Bangalore").toString().contains("58"));
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByCount() : Count for the city Bangalore does not match the expected value",
				dataSet.get("Bloemfontein").toString().contains("2"));
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByCount() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.get("Kanpur").toString().contains("2"));

		assertTrue("testSelectColumnsWithoutWhereWithGroupByCount() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testSelectColumnsWithoutWhereWithGroupByCount() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testSelectColumnsWithoutWhereWithGroupByCount", dataSet);

	}

	@Test
	public void testSelectColumnsWithoutWhereWithGroupBySum() throws FileNotFoundException {

		int totalRecordsExpected = 31;

		HashMap dataSet = query.executeQuery("select city,sum(season) from data/ipl.csv group by city");

		assertNotNull("testSelectColumnsWithoutWhereWithGroupBySum() : Empty Dataset returned", dataSet);
		assertEquals("testSelectColumnsWithoutWhereWithGroupBySum() : Total number of records should be 31",
				totalRecordsExpected, dataSet.size());

		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupBySum() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.get("Bangalore").toString().contains("116725"));
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupBySum() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.get("Bloemfontein").toString().contains("4018"));
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupBySum() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.get("Kanpur").toString().contains("4032"));

		assertTrue("testSelectColumnsWithoutWhereWithGroupBySum() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testSelectColumnsWithoutWhereWithGroupBySum() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testSelectColumnsWithoutWhereWithGroupBySum", dataSet);

	}

	@Test
	public void testSelectColumnsWithoutWhereWithGroupByMin() throws FileNotFoundException {

		int totalRecordsExpected = 31;

		HashMap dataSet = query.executeQuery("select city,min(season) from data/ipl.csv group by city");
		assertEquals("testSelectColumnsWithoutWhereWithGroupByMin() : Total number of records should be 31",
				totalRecordsExpected, dataSet.size());
		assertNotNull("testSelectColumnsWithoutWhereWithGroupByMin() : Empty Dataset returned", dataSet);

		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByMin() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.get("Bangalore").toString().contains("2008"));
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByMin() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.get("Bloemfontein").toString().contains("2009"));
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByMin() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.get("Kanpur").toString().contains("2016"));

		assertTrue("testSelectColumnsWithoutWhereWithGroupByMin() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testSelectColumnsWithoutWhereWithGroupByMin() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testSelectColumnsWithoutWhereWithGroupByMin", dataSet);

	}

	@Test
	public void testSelectColumnsWithoutWhereWithGroupByMax() throws FileNotFoundException {

		int totalRecordsExpected = 31;

		HashMap dataSet = query.executeQuery("select city,max(win_by_wickets) from data/ipl.csv group by city");

		assertEquals("testSelectColumnsWithoutWhereWithGroupByMax() : Total number of records should be 31",
				totalRecordsExpected, dataSet.size());
		assertNotNull("testSelectColumnsWithoutWhereWithGroupByMax() : Empty Dataset returned", dataSet);

		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByMax() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.get("Bangalore").toString().contains("10"));
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByMax() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.get("Bloemfontein").toString().contains("6"));
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByMax() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.get("Kanpur").toString().contains("6"));

		assertTrue("testSelectColumnsWithoutWhereWithGroupByMax() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testSelectColumnsWithoutWhereWithGroupByMax() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testSelectColumnsWithoutWhereWithGroupByMax", dataSet);

	}

	@Test
	public void testSelectColumnsWithoutWhereWithGroupByAvg() throws FileNotFoundException {

		int totalRecordsExpected = 31;
		JsonWriter js = new JsonWriter();

		HashMap dataSet = query.executeQuery("select city,avg(win_by_wickets) from data/ipl.csv group by city");

		assertEquals("testSelectColumnsWithoutWhereWithGroupByAvg() : Total number of records should be 31",
				totalRecordsExpected, dataSet.size());
		assertNotNull("testSelectColumnsWithoutWhereWithGroupByAvg() : Empty Dataset returned", dataSet);
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByAvg() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.get("Bangalore").toString().contains("3.4827586206896552"));
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByAvg() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.get("Bloemfontein").toString().contains("3.0"));
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByAvg() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.get("Kanpur").toString().contains("6.0"));
		assertTrue("testSelectColumnsWithoutWhereWithGroupByAvg() : Writing data into json format has failed",
				jsonWriter.writeToJson(dataSet));
		assertTrue("testSelectColumnsWithoutWhereWithGroupByAvg() : Json file is empty, no data is written",
				(new File("data/result.json").length() > 0));

		display("testSelectColumnsWithoutWhereWithGroupByAvg", dataSet);

	}

	private void display(String testCaseName, HashMap dataSet) {

		System.out.println(testCaseName);
		System.out.println("================================================================");
		System.out.println(dataSet);

	}

}