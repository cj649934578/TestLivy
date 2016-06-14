package main;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;

import scala.Tuple2;

public class JavaPi2 implements Job<Double> {

	
	private JavaSparkContext sssc;
	private SQLContext sqlcontext;

	public Double test() {

		// SparkConf conf = new
		// SparkConf().setAppName("appname").setMaster("mesos://192.168.1.45:5050");
		// JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> textFile = sssc.textFile("hdfs://192.168.1.170:9000/aaa.txt");



		Double asd = Double.valueOf(textFile.count());
		// sc.close();
		return asd;
	}

	@Override
	public Double call(JobContext arg0) throws Exception {
		sssc = arg0.sc();
		sqlcontext = arg0.sqlctx();
		return test();
	}

}
