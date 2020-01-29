package edu.ucsb.mnl.mona;

import java.io.IOException;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebListener;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Servlet implementation class SparkDataServlet
 */
@WebServlet("/sparkdataservlet")
public class SparkDataServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
    private static SparkSession spark = null; 
    
    /**
     * @see HttpServlet#HttpServlet()
     */
    public SparkDataServlet() {
        super();
    }
    
    public static SparkSession GetSpark() {
    	return spark;
    }
    
    public static void connect() {
    	if ( spark == null ) {
    		System.out.println("Making new Spark connection...");
    		spark = SparkSession.builder().appName("Test App")
    				.config("spark.cassandra.connection.host", "169.231.235.242")
    				.config("spark.cassandra.input.split.size_in_mb", "200")
    				.master("spark://master:5100")
    				.getOrCreate();
    	}

    }
    
    public static void disconnect() {
    	if ( spark != null ) {
    		spark.close();
    	}
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		connect();
        response.getWriter().append("Connection established.");
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}

}
