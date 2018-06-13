package com.stackroute.datamunger.writer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JsonWriter {
	/*
	 * this method will write the resultSet object into a JSON file. On successful
	 * writing, the method will return true, else will return false
	 */
	public boolean writeToJson(Map resultSet) {

		BufferedWriter writer = null;
		/*
		 * Gson is a third party library to convert Java object to JSON. We will use
		 * Gson to convert resultSet object to JSON
		 */
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String result = gson.toJson(resultSet);
		/*
		 * write JSON string to data/result.json file. As we are performing File IO,
		 * consider handling exception
		 */
		try {
			writer = new BufferedWriter(new FileWriter("data/result.json"));
			writer.write(result);
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("File writing failed");

			return false;
		} finally {
			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

}
