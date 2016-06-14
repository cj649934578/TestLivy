package main;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import com.cloudera.livy.LivyClient;
import com.cloudera.livy.LivyClientBuilder;

public class Testfunction {

	
	
	public Double getValue() throws Exception {
		Double reval = (double) 0;
		LivyClient client = new LivyClientBuilder()
				  .setURI(new URI("http://192.168.1.47:8998"))
				  .build();

				String piJar = "hdfs://192.168.1.170:9000/Pooc21.jar";
//		String piJar = "/home/user1/Pooc18.jar";
				try {
				  System.err.printf("Uploading %s to the Spark context...\n", piJar);
				  
				  URI uri = new URI(piJar);
				  client.addJar(uri);
//				  client.uploadJar(new File(piJar)).get();

				  int samples = 10;
				  
				  System.err.printf("Running PiJob with %d samples...\n", samples);
				  double pi = client.submit(new JavaPi2()).get();
				  reval = pi;
				  System.out.println("Pi is roughly: " + pi);
				} finally {
				  client.stop(true);
				}
				return reval;
	}
	
}
