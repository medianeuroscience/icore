package edu.ucsb.mnl.mona;

import java.io.IOException;
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
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Servlet implementation class EventByDay
 */
@WebServlet("/eventbyday")
public class EventByDay extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private String dataResponse = "";
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public EventByDay() {
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
        map.put("table","event_by_day");

		Dataset<Row> dataset = SparkDataServlet.GetSpark().read().format("org.apache.spark.sql.cassandra").options(map).load();
//		dataset.createOrReplaceTempView("event_by_day");
		System.out.println("Executing query...");
		Dataset<Row> query = dataset.filter("event_root_code='14'").filter("action_geo_name='US'").filter("event_day > '" + minDate + "'").filter("event_day <= '" + maxDate + "'");
//		response.getWriter().append(query.showString(1000, 0, false));
		
		System.out.println("Collecting...");
		Row[] results = (Row[]) query.collect();
		
		System.out.println("Writing...");
		dataResponse = Arrays.toString( results[0].schema().fieldNames() ) + "\n";
		for ( int i = 0; i < results.length; i++ ) {
			dataResponse += results[i].toString() + "\n";
		}
		response.getWriter().append(dataResponse);
		
	} 

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
