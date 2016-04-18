package edu.brown.cs.systems.tpcds.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;

import com.databricks.spark.sql.perf.tpcds.Tables;

import edu.brown.cs.systems.tpcds.Dsdgen;

public class SparkTPCDSDataGenerator {

	public static void generateData() {
		generateData(TPCDSSettings.dataLocation(), TPCDSSettings.dataFormat(),
				TPCDSSettings.scaleFactor());
	}

	public static void generateData(String location, String format, int scaleFactor) {
		generateData(location, format, scaleFactor, TPCDSSettings.overwrite(),
				TPCDSSettings.partitionTables(), TPCDSSettings.useDoubleForDecimal(),
				TPCDSSettings.clusterByPartitionColumns(), TPCDSSettings.filterOutNullPartitionValues());
	}

	public static void generateData(String location, String format, int scaleFactor,
			boolean overwrite, boolean partitionTables, boolean useDoubleForDecimal, boolean clusterByPartitionColumns,
			boolean filterOutNullPartitionValues) {
		SparkConf conf = new SparkConf().setAppName("TPC-DS generateData");
		SparkContext sc = new SparkContext(conf);
		SQLContext sql = new SQLContext(sc);
		Tables tables = new Tables(sql, scaleFactor);
		tables.genData(location, format, overwrite, partitionTables, useDoubleForDecimal, clusterByPartitionColumns,
				filterOutNullPartitionValues, "");

		sc.stop();
	}

	public static void main(String[] args) {
		generateData();
	}

}
