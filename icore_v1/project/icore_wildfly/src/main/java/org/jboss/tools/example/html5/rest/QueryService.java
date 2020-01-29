package org.jboss.tools.example.html5.rest;

import javax.ejb.Stateful;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.jboss.tools.example.html5.data.SparkConnector;

/**
 * JAX-RS Example
 * <p/>
 * This class produces a RESTful service to read/write the contents of the members table.
 */
@Path("/query")
@RequestScoped
@Stateful
public class QueryService {

//	@Inject
//	private SparkConnector sc;
	
	/**
     * Writes hello to the console
     */
    @GET
    @Path("/hello")
    @Produces(MediaType.APPLICATION_JSON)
    public String hellojboss() {

        return ("{\"message\":\"Hello!\"}");
    }
    
    /**
     * Writes eventByDay raw text to the console
     */
    @GET
    @Path("/eventbyday")
    @Produces(MediaType.TEXT_PLAIN)
    public String eventbyday() {
    	//return ("Stub");
        return ( SparkConnector.GetEventByDay() );
    }
	
}
