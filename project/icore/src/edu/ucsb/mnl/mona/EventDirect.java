package edu.ucsb.mnl.mona;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.sql.Dataset;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
/**
 * Servlet implementation class EventByDay
 */
@WebServlet("/eventdirect")
public class EventDirect extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private String dataResponse = "";
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public EventDirect() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		Cluster cassandraCluster = Cluster.builder().addContactPoint("169.231.235.242").build();
		Session cassandraSession = cassandraCluster.connect();
		
		String minDate = request.getParameter("minDate");
		String maxDate = request.getParameter("maxDate");
		
		System.out.println("Executing Cassandra query...");
		List<Row> rows = cassandraSession.execute(
				"SELECT event_id, event_day, source_url, num_sources, event_tone_avg "
				+ "FROM gdelt.event_by_day where event_day > '" + minDate + "' AND event_day <= '" + maxDate + "' "
				+ "AND action_geo_name='US' AND event_root_code='14' ALLOW FILTERING;"
			).all();
		
		System.out.println("Found " + rows.size() + " results...");
		dataResponse = "id,date,url,tone,num_sources,event_tone_avg\n";
		for ( int i = 0; i < rows.size(); i++ ) {
			dataResponse += rows.get(i).getString(0) + "," + 
					rows.get(i).getTimestamp(1) + "," +
					rows.get(i).getString(2) + "," + 
					rows.get(i).getInt(3) + "," + 
					rows.get(i).getFloat(4) +
					"\n";
		}
		
		cassandraSession.close();
		cassandraCluster.close();
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
