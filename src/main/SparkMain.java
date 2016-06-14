package main;

import java.util.Arrays;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkMain {

	public static void main(String[] args) {

		Properties props = System.getProperties();
		props.setProperty("MESOS_NATIVE_JAVA_LIBRARY", "/home/user1/Downloads/mesos-0.28.1/build/src/.libs/libmesos.so");
		
		SparkConf conf = new SparkConf().setMaster("mesos://192.168.1.45:5050").setAppName("test1").set("spark.executor.uri", "hdfs://192.168.1.170:9000/spark/spark-1.6.1-bin-hadoop2.6.tgz").set("spark.mesos.executor.home", "/home/user1/Downloads/spark-1.6.1-bin-hadoop2.6");

		JavaSparkContext sc = new JavaSparkContext(conf); // An existing
															// JavaSparkContext.
		JavaRDD<String> textFile = sc.textFile("hdfs://192.168.1.170:9000/aaa.csv");
		JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) {
				return Arrays.asList(s.split(" "));
			}
		});
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		});
		System.out.println(counts.count());

	}

}
