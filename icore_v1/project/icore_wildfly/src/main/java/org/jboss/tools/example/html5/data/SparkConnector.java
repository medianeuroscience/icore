package org.jboss.tools.example.html5.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Consumer;


//@ApplicationScoped
public class SparkConnector {
	
	public static String GetEventByDay() {
		
		String returnValue = "";
		//	Temporary:  make a new JVM for each Spark query
		try {
//			Process p = Runtime.getRuntime().exec(new String[]{"sh","-c","/home/mona/testconnection/runtest.sh"});
//			StreamGobbler streamGobbler = new StreamGobbler(p.getInputStream(), System.out::println);
//			Executors.newSingleThreadExecutor().submit(streamGobbler);
//			int exitCode = p.waitFor();
//			assert exitCode == 0;
			
			ProcessBuilder pb = new ProcessBuilder("/home/mona/testconnection/runtest.sh");
			Map<String, String> env = pb.environment();
			pb.directory(new File("/home/mona/testconnection"));
			Process p = pb.start();
			int exitCode = p.waitFor();
			assert exitCode == 0;

			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

			String s;
			while ((s = stdInput.readLine()) != null) {
				returnValue += s + '\n';
	        }
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return returnValue;
	}

//	private static class StreamGobbler implements Runnable {
//	    private InputStream inputStream;
//	    private Consumer<String> consumer;
//	 
//	    public StreamGobbler(InputStream inputStream, Consumer<String> consumer) {
//	        this.inputStream = inputStream;
//	        this.consumer = consumer;
//	    }
//	 
//	    @Override
//	    public void run() {
//	        new BufferedReader(new InputStreamReader(inputStream)).lines().forEach(consumer);
//	    }
//	}


	
}



