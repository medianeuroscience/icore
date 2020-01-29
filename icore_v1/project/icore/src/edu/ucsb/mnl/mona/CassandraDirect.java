package edu.ucsb.mnl.mona;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.*;

import edu.ucsb.mnl.mona.gcam.GCAMBeanList;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * Servlet implementation class EventByDay
 */
@WebServlet("/direct")
public class CassandraDirect extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private String dataResponse = "";
	private static List<String> mfd = new ArrayList<String>();
	private static List<String> mfdLabels = new ArrayList<String>();
	private static List<String> liwc = new ArrayList<String>();
	private static List<String> liwcLabels = new ArrayList<String>();
	
    /**
     * @see HttpServlet#HttpServlet()
     */
    public CassandraDirect() {
        super();
        
//        GCAMBeanList gcbl = GKGDirect.readGCAMBeans("/home/tomcat/gcam_mappings.json");
//        for ( int i = 0; i < gcbl.data.size(); i++ ) {
//        	gcam.put( gcbl.data.get(i).Variable, gcbl.data.get(i).DimensionHumanName );
//        }
        
    	mfd.add("c25.1"); mfdLabels.add("Care");
		mfd.add("c25.2"); mfdLabels.add("Harm");
		mfd.add("c25.3"); mfdLabels.add("Fairness");
		mfd.add("c25.4"); mfdLabels.add("Cheating");
		mfd.add("c25.5"); mfdLabels.add("Loyalty");
		mfd.add("c25.6"); mfdLabels.add("Betrayal");
		mfd.add("c25.7"); mfdLabels.add("Authority");
		mfd.add("c25.8"); mfdLabels.add("Subversion");
		mfd.add("c25.9"); mfdLabels.add("Purity");
		mfd.add("c25.10"); mfdLabels.add("Degradation");
		mfd.add("c25.11"); mfdLabels.add("MoralityGeneral");
		
		liwc.add("c5.1"); liwcLabels.add("Assent");
		liwc.add("c5.2"); liwcLabels.add("Death");
		liwc.add("c5.3"); liwcLabels.add("Religion");
		liwc.add("c5.4"); liwcLabels.add("Money");
		liwc.add("c5.5"); liwcLabels.add("Home");
		liwc.add("c5.6"); liwcLabels.add("Leisure");
		liwc.add("c5.7"); liwcLabels.add("Achievement");
		liwc.add("c5.8"); liwcLabels.add("Work");
		liwc.add("c5.9"); liwcLabels.add("Time");
		liwc.add("c5.10"); liwcLabels.add("Space");
		liwc.add("c5.11"); liwcLabels.add("Motion");
		liwc.add("c5.12"); liwcLabels.add("Relativeness");
		liwc.add("c5.13"); liwcLabels.add("Ingest");
		liwc.add("c5.14"); liwcLabels.add("Sexual");
		liwc.add("c5.15"); liwcLabels.add("Health");
		liwc.add("c5.16"); liwcLabels.add("Body");
		liwc.add("c5.17"); liwcLabels.add("Biology");
		liwc.add("c5.18"); liwcLabels.add("Feel");
		liwc.add("c5.19"); liwcLabels.add("Hear");
		liwc.add("c5.20"); liwcLabels.add("See");
		liwc.add("c5.21"); liwcLabels.add("Perception");
		liwc.add("c5.22"); liwcLabels.add("Exclusion");
		liwc.add("c5.23"); liwcLabels.add("Inclusion");
		liwc.add("c5.24"); liwcLabels.add("Inhibition");
		liwc.add("c5.25"); liwcLabels.add("Certain");
		liwc.add("c5.26"); liwcLabels.add("Tentative");
		liwc.add("c5.27"); liwcLabels.add("Discrepency");
		liwc.add("c5.28"); liwcLabels.add("Cause");
		liwc.add("c5.29"); liwcLabels.add("Insight");
		liwc.add("c5.30"); liwcLabels.add("CogMech");
		liwc.add("c5.31"); liwcLabels.add("Sad");
		liwc.add("c5.32"); liwcLabels.add("Anger");
		liwc.add("c5.33"); liwcLabels.add("Anxiety");
		liwc.add("c5.34"); liwcLabels.add("NegativeEmotion");
		liwc.add("c5.35"); liwcLabels.add("PositiveEmotion");
		liwc.add("c5.36"); liwcLabels.add("Affect");
		liwc.add("c5.37"); liwcLabels.add("Humans");
		liwc.add("c5.38"); liwcLabels.add("Friends");
		liwc.add("c5.39"); liwcLabels.add("Family");
		liwc.add("c5.40"); liwcLabels.add("Social");
		liwc.add("c5.41"); liwcLabels.add("Swear");
		liwc.add("c5.42"); liwcLabels.add("Numbers");
		liwc.add("c5.43"); liwcLabels.add("Quant");
		liwc.add("c5.44"); liwcLabels.add("Negate");
		liwc.add("c5.45"); liwcLabels.add("Conj");
		liwc.add("c5.46"); liwcLabels.add("Prep");
		liwc.add("c5.47"); liwcLabels.add("Adverbs");
		liwc.add("c5.48"); liwcLabels.add("Future");
		liwc.add("c5.49"); liwcLabels.add("Present");
		liwc.add("c5.50"); liwcLabels.add("Past");
		liwc.add("c5.51"); liwcLabels.add("AuxVb");
		liwc.add("c5.52"); liwcLabels.add("Verbs");
		liwc.add("c5.53"); liwcLabels.add("Article");
		liwc.add("c5.54"); liwcLabels.add("IPron");
		liwc.add("c5.55"); liwcLabels.add("They");
		liwc.add("c5.56"); liwcLabels.add("SheHe");
		liwc.add("c5.57"); liwcLabels.add("You");
		liwc.add("c5.58"); liwcLabels.add("We");
		liwc.add("c5.59"); liwcLabels.add("I");
		liwc.add("c5.60"); liwcLabels.add("PersonalPron");
		liwc.add("c5.61"); liwcLabels.add("Pronoun");
		liwc.add("c5.62"); liwcLabels.add("Funct");
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		try {
			String table = request.getParameter("data");
			String year = request.getParameter("year");
			String monthString = request.getParameter("month");		
			String sourceFilter = request.getParameter("sources");
			String entityFilter = request.getParameter("entities");
			String themeFilter = request.getParameter("theme");
			String locationFilter = request.getParameter("location");
			
			if ( year == null )
				year = "" + new Date().getYear();
			
			if (table == null )
				table = "gkg";
			
			int minMonth = 1;
			int maxMonth = 12;
			if ( monthString == null)
				monthString = "1-12";
			if ( monthString.contains("-")) {
				minMonth = Integer.valueOf(monthString.split("-")[0]);
				maxMonth = Integer.valueOf(monthString.split("-")[1]);
			}
			else {
				minMonth = Integer.valueOf(monthString);
				maxMonth = minMonth + 1;
			}
			
			String dirPath = "/mnt/apicache/";
			String fileName = "" + table + "-" + year + "." + minMonth + "." + maxMonth + "." + sourceFilter + "." + entityFilter + "." + themeFilter + "." + locationFilter + ".csv";
			
			if ( entityFilter != null )
				entityFilter = entityFilter.replaceAll( "_", " " );
			
			File check = new File(dirPath + fileName);
			if (!check.exists() ) {
			
				SocketOptions so = new SocketOptions();
				so.setReadTimeoutMillis(1000000);
				so.setConnectTimeoutMillis(400000);
				Cluster cassandraCluster = Cluster.builder().addContactPoint("169.231.235.242").withSocketOptions(so).build();
				cassandraCluster.getConfiguration().getQueryOptions().setDefaultIdempotence(true);
				Session cassandraSession = cassandraCluster.connect();
				
				if (table.equals("gkg"))
					queryGKG( cassandraSession, dirPath, fileName, sourceFilter, entityFilter, themeFilter, locationFilter, year, minMonth, maxMonth );
				else if (table.equals("event"))
					queryEvent(cassandraSession, dirPath, fileName, year, minMonth, maxMonth);
				
	
				cassandraSession.close();
				cassandraCluster.close();
				
				Files.setPosixFilePermissions(Paths.get(dirPath + fileName), PosixFilePermissions.fromString("rw-r--r--"));
			}
			
			dataResponse = "http://169.231.235.96/" + fileName;		
			response.sendRedirect(dataResponse);
			//response.getWriter().append(dataResponse);
		}
		catch (NoHostAvailableException e ) {
			e.printStackTrace();
			System.out.println(e.getErrors());
			PrintWriter out = response.getWriter();
			 
	        out.println(e.getErrors());
	        out.close();
		}
	}
	
	public void queryGKG( Session cassandraSession, String dirPath, String fileName, 
			String sourceFilter, String entitiesFilter, String themeFilter, String locationFilter, String year, int minMonth, int maxMonth ) throws FileNotFoundException, UnsupportedEncodingException, IOException {
		
		FileWriter outputFile = new FileWriter(dirPath + fileName);
		BufferedWriter os = new BufferedWriter( outputFile );
//		PrintWriter resultWriter = new PrintWriter(dirPath + fileName,"UTF-8");
//		resultWriter.println( "id,date,url,tone,mft_data,gcam_data,themes, named_entities, source" );
		os.write("id,date,url,tone,");
		
		for ( int i = 0; i < mfdLabels.size(); i++ ) {
			os.write(mfdLabels.get(i).toLowerCase() + ",");
		}
		
		for ( int i = 0; i < liwcLabels.size(); i++ ) {
			os.write(liwcLabels.get(i).toLowerCase() + ",");
		}
		os.write("themes,named_entities,source,wordcount,source_location");
		os.newLine();
		
		String suffix = (maxMonth != 12 ) ? ("< '" + year + "-" + (maxMonth+1) + "-01'  ") : ("<= '" + year + "-" + maxMonth + "-31' ");
		String cql = "SELECT gkg_id, gkg_day, url, tone_avg, mft_data,gcam_data, themes, named_entities, source, wordcount, source_location "
				+ "FROM gdelt.gkg_record_by_day "
				+ "WHERE gkg_day >= '" + year + "-" + minMonth + "-01' AND gkg_day " + suffix
				+ (themeFilter != null ? "AND themes CONTAINS '" + themeFilter + "' " : "")
				+ (locationFilter != null ? "AND source_location='" + locationFilter + "' " : "" )
				+ (entitiesFilter != null ? "AND named_entities CONTAINS '" + entitiesFilter + "' " : "" )
				+ "ALLOW FILTERING;";
		System.out.println(cql);
		List<Row> rows = cassandraSession.execute(cql).all();
		
		HashSet<String> filterSet = null;
		System.out.println("Found " + rows.size() + " results, filtering...");
		if ( sourceFilter != null) { 
			sourceFilter = sourceFilter.replaceAll("\\(", "" );
			sourceFilter = sourceFilter.replaceAll("\\)", "" );
			String[] sourceFilterString = sourceFilter.split(",");
			System.out.println( "Using " + Arrays.toString( sourceFilterString ) + " as filter.");
			filterSet = new HashSet<String>( Arrays.asList( sourceFilterString ) );
		}
		
		System.out.println( "Writing month " + minMonth + "-" + maxMonth + " to " + fileName);
		for ( int i = 0; i < rows.size(); i++ ) {
			if ( filterSet == null || filterSet.contains( rows.get(i).getString(8) ) ) {
				os.write(
						rows.get(i).getString(0) + "," + 
						rows.get(i).getTimestamp(1) + "," + 
						rows.get(i).getString(2) + "," + 
						rows.get(i).getFloat(3) + "," +
						findCodes( rows.get(i).getMap(4, String.class, String.class), mfd ) +
						findCodes( rows.get(i).getMap(5, String.class, String.class), liwc ) +
						rows.get(i).getSet(6, String.class).toString().replaceAll(",", "|").replaceAll("\\s+","") + "," +
						rows.get(i).getSet(7, String.class).toString().replaceAll(",", "|").replaceAll("\\s+","") + "," +
						rows.get(i).getString(8) + "," +
						rows.get(i).getInt(9) + "," +
						rows.get(i).getString(10)
					);
				os.newLine();
			}
			
		}
		
		os.flush();

		os.close();
		System.out.println("Finished writing");
	}
	
	public void queryEvent(Session cassandraSession, String dirPath, String fileName, String year, int minMonth, int maxMonth) throws FileNotFoundException, UnsupportedEncodingException {
		
//		month = 1;
		String suffix = (maxMonth != 12 ) ? ("< '" + year + "-" + (maxMonth+1) + "-01'  ") : ("<= '" + year + "-" + maxMonth + "-31' ");
		
		String cql = "SELECT event_id, event_day, source_url, num_sources, event_tone_avg, action_geo_name "
				+ "FROM gdelt.event_by_day where event_day > '" + year + "-" + minMonth + "-01' AND event_day " + suffix
				+ "AND event_root_code='14' ALLOW FILTERING;";
		
		System.out.println("Executing Cassandra query on event...");
		System.out.println(cql);
		List<Row> rows = cassandraSession.execute(
				cql
			).all();
		
		System.out.println( "Writing to " + fileName);
		PrintWriter resultWriter = new PrintWriter(dirPath + fileName,"UTF-8");
		resultWriter.println("id,date,url,num_sources,event_tone_avg");
		for ( int i = 0; i < rows.size(); i++ ) {
			if ( rows.get(i).getString(5).contains("US")) {
				resultWriter.println(
						rows.get(i).getString(0) + "," + 
						rows.get(i).getTimestamp(1) + "," +
						rows.get(i).getString(2) + "," + 
						rows.get(i).getInt(3) + "," + 
						rows.get(i).getFloat(4) );
			}
		}
		resultWriter.close();
		System.out.println("Finished writing");
	}
	
	public static String findCodes( Map<String,String> map, List<String> codes ) {

		String resultString = "";
		for ( int i = 0; i < codes.size(); i++ ) {
			if ( map.containsKey(codes.get(i))) {
				resultString += map.get(codes.get(i)) + ",";
			}
			else
				resultString += ",";
		}
		return resultString;
	}
	
	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}
	
	public static GCAMBeanList readGCAMBeans(String filename) {
		Gson gson = new Gson();
		try {
			BufferedReader br = new BufferedReader( new FileReader(filename));
			GCAMBeanList gcam_list = gson.fromJson( br, GCAMBeanList.class);
			return gcam_list;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

}
