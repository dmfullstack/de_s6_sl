package com.stackroute.datamunger;

import java.io.FileNotFoundException;
import java.util.Scanner;

import com.stackroute.datamunger.query.Query;
import com.stackroute.datamunger.writer.JsonWriter;


public class DataMunger {
	
	public static void main(String[] args){
		
		Scanner scanner=new Scanner(System.in);
		String queryString;
		//read the query from the user
		System.out.println("Please enter your query:");
		System.out.println(">");
		queryString=scanner.nextLine();
		/*
		 * Instantiate Query class. This class is responsible for: 1. Parsing the query
		 * 2. Select the appropriate type of query processor 3. Get the resultSet which
		 * is populated by the Query Processor
		 */
		Query query=new Query();
		
		/*
		 * Instantiate JsonWriter class. This class is responsible for writing the
		 * ResultSet into a JSON file
		 */
		JsonWriter writer=new JsonWriter();
		/*
		 * call executeQuery() method of Query class to get the resultSet. Pass this
		 * resultSet as parameter to writeToJson() method of JsonWriter class to write
		 * the resultSet into a JSON file
		 */
		try {
			if (writer.writeToJson(query.executeQuery(queryString))) {
				System.out.println("Output written to data/result.json");
			}
		} catch (FileNotFoundException e) {
			System.out.println("File not found");
		}

	}
}
