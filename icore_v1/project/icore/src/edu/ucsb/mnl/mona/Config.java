package edu.ucsb.mnl.mona;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

@WebListener
public class Config implements ServletContextListener {

	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		SparkDataServlet.disconnect();
		System.out.println("DISCONNECTED");
		
	}

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		SparkDataServlet.connect();
		System.out.println("CONNECTION INITIALIZED");
	}

}
