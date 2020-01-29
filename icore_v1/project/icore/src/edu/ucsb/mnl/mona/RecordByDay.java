package edu.ucsb.mnl.mona;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;

import com.datastax.spark.connector.cql.CassandraConnector;

/**
 * Servlet implementation class EventByDay
 */
@WebServlet("/recordbyday")
public class RecordByDay extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private String dataResponse = "";
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public RecordByDay() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		String minDate = request.getParameter("minDate");
		String maxDate = request.getParameter("maxDate");
		
		HashMap<String, String> map = new HashMap<String,String>();
        map.put("keyspace","gdelt");
        map.put("table","gkg_record_by_day");
		
        System.out.println("Creating Temporary View...");
		Dataset<Row> dataset = SparkDataServlet.GetSpark().read().format("org.apache.spark.sql.cassandra").options(map).load();
//		dataset.createOrReplaceTempView("gkg_record_by_day");
		
		System.out.println("Creating SQL Query...");
//		Dataset<Row> eventByDay = SparkDataServlet.GetSpark().sql(
//				"SELECT gkg_id, gkg_day, url, tone_avg, themes, source "
//				+ "FROM gkg_record_by_day "
//				+ "WHERE gkg_day > '" + minDate + "' AND gkg_day <= '" + maxDate + "'  "
//				+ "AND source IN ('breitbart.com', 'foxnews.com', 'nytimes.com', 'huffingtonpost.com')");
//		AND source_location='US'
//		AND ARRAY_CONTAINS(themes, 'ENV_CLIMATECHANGE') 
		
		Dataset<Row> query = dataset
				.filter("gkg_day > '" + minDate + "'")
				.filter("gkg_day <= '" + maxDate + "'")
				.filter("source_location = 'US'")
				.filter("ARRAY_CONTAINS(themes, 'IMMIGRATION')");

		
		String dirPath = "/event_data/apicache/";
		String fileName = "sparktest.result";
		System.out.println( "Writing to " + fileName);
		PrintWriter resultWriter = new PrintWriter(dirPath + fileName,"UTF-8");
		resultWriter.println(query.showString(10000, 0, false));
		
		resultWriter.close();
		System.out.println("Finished writing");
		
		System.out.println("Showing 100 results...");
		response.getWriter().append("Result written to " + dirPath + fileName );
		
//		System.out.println("Collecting query...");
//		Row[] results = (Row[]) query.collect();
//		
//		System.out.println("Writing...");
//		dataResponse = Arrays.toString( results[0].schema().fieldNames() ) + "\n";
//		for ( int i = 0; i < results.length; i++ ) {
//			dataResponse += results[i].toString() + "\n";
//		}
//		response.getWriter().append(dataResponse);
	}
	
	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
