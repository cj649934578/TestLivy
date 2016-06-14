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

public class JavaPi implements Job<Double>{

	private JavaSparkContext sssc;
	private SQLContext sqlcontext;
	
	public Double test() {

		
//		SparkConf conf = new SparkConf().setAppName("appname").setMaster("mesos://192.168.1.45:5050");
//		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		JavaRDD<String> textFile = sssc.textFile("hdfs://192.168.1.170:9000/aaa.csv");

		// The schema is encoded in a string
		String schemaString = "sn name col1 col2";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName: schemaString.split(" ")) {
		  fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD (people) to Rows.
		JavaRDD<Row> rowRDD = textFile.map(
		  new Function<String, Row>() {
		    public Row call(String record) throws Exception {
		      String[] fields = record.split(",");
		      return RowFactory.create(fields[0], fields[1].trim(),fields[2].trim(),fields[3].trim());
		    }
		  });

		// Apply the schema to the RDD.
		DataFrame peopleDataFrame = sqlcontext.createDataFrame(rowRDD, schema);
		
		// Register the DataFrame as a table.
		peopleDataFrame.registerTempTable("people");

		// SQL can be run over RDDs that have been registered as tables.
		DataFrame results = sqlcontext.sql("SELECT name FROM people");

		// The results of SQL queries are DataFrames and support all the normal RDD operations.
		// The columns of a row in the result can be accessed by ordinal.
		List<String> names = results.javaRDD().map(new Function<Row, String>() {
		  public String call(Row row) {
		    return "Name: " + row.getString(0);
		  }
		}).collect();
		
		
		System.out.println("dddd" + textFile.count());
		
		Double asd = Double.valueOf(names.size());
//		sc.close();
		return asd;
	}

	@Override
	public Double call(JobContext arg0) throws Exception {
		sssc = arg0.sc();
		sqlcontext = arg0.sqlctx();
		return test();
	}

}
