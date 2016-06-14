package main;

import java.io.File;
import java.net.URI;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.livy.LivyClient;
import com.cloudera.livy.LivyClientBuilder;

public class Ppoc {

	public static void main(String[] args) throws Exception {

		Testfunction tt = new Testfunction();
		System.out.println(tt.getValue());
		
	}

}
