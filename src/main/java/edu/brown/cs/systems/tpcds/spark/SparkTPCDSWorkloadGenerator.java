package edu.brown.cs.systems.tpcds.spark;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import edu.brown.cs.systems.tpcds.QueryUtils;
import edu.brown.cs.systems.tpcds.QueryUtils.Benchmark;
import edu.brown.cs.systems.tpcds.QueryUtils.Benchmark.Query;

import com.databricks.spark.sql.perf.tpcds.Tables;

public class SparkTPCDSWorkloadGenerator {
	
	public static final Logger log = LoggerFactory.getLogger(SparkTPCDSWorkloadGenerator.class);
	
	/** Load tpcds tables into memory using default configuration.
	 * Creates the spark context and sql context
	 * @return SQL context with tables loaded
	 */
	public static SQLContext spinUpWithDefaults() {
		return spinUp("SparkTPCDSWorkloadGenerator", TPCDSSettings.createWithDefaults());
	}
	
	public static SQLContext spinUp(String name, TPCDSSettings settings) {
		SparkConf c = new SparkConf().setAppName(name);
		SparkContext sc = new SparkContext(c);
		SQLContext sqlContext = new HiveContext(sc);
		loadExistingTablesIntoMemory(sqlContext, settings);
		return sqlContext;
	}
	
	/** Loads tpcds tables into memory in Spark from a source location, eg from HDFS.
	 * Only uses the dataLocation and dataFormat settings. */
	public static void loadExistingTablesIntoMemory(SQLContext sqlContext, TPCDSSettings settings) {
		/* Tables constructor takes dsdgenDir and scaleFactor, but they are not used when loading existing data.
		 * So we just use default values for these instead of adding them as confusing and unused parameters */
		Tables tables = new Tables(sqlContext, settings.scaleFactor);
		tables.createTemporaryTables(settings.dataLocation, settings.dataFormat, "");
	}
	
	
	public static void main(String[] args) throws FileNotFoundException {
		System.out.println("Starting SparkTPCDSWorkloadGenerator");
		
		SQLContext sql = spinUpWithDefaults();
		Map<String, Benchmark> allBenchmarks = QueryUtils.load();
		String benchmarkName = "impala-tpcds-modified-queries";
		String queryName = "q19.sql";
		Benchmark benchmark = allBenchmarks.get(benchmarkName);
		Query query = benchmark.benchmarkQueries.get(queryName);
		System.out.printf("Running query %s/%s\n", query);
		System.out.println(query.queryText);
		Row[] rows = sql.sql(query.queryText).collect();
		for (Row r : rows) {
			System.out.println(r);
		}
		
//		for (String queryName : Queries.all()) {
//			try {
//				String queryText = Queries.loadQuery(queryName);
//				queries.put(queryName, queryText);
//				System.out.println("Executing " + queryName);
//				sql.sql(queryText);
//				successful.add(queryName);
//				System.out.println(queryName + " succeeded.");
//			} catch (Throwable t) {
//				failures.add(queryName);
//				reasons.put(queryName, t);
//				System.out.println(queryName + " failed.");
//			}
//		}
//
//		System.out.println("Failure reasons:");
//		for (String queryName : reasons.keySet()) {
//			System.out.println(queryName);
//			reasons.get(queryName).printStackTrace();
//			System.out.println();
//			System.out.println();
//		}
//		System.out.println();
//		System.out.println();
//		System.out.println();
//		System.out.println("Successful:");
//		System.out.println(StringUtils.join(successful, "\n"));
//		System.out.println();
//		System.out.println();
//		System.out.println();
//		System.out.println("Failed:");
//		System.out.println(StringUtils.join(failures, "\n"));
//		System.out.println();
//		
//		System.out.printf("%d successful, %d failures\n", successful.size(), failures.size());
		
//		
////		loadExistingTablesIntoMemory(sql, dataLocation, dataFormat);
//		
//		TPCDS tpcds = new TPCDS (sql);
//		tpcds.tpcds1_4QueriesMap().get("q7");
//		
//		Seq<Benchmarkable> allQueries = tpcds.allQueries();
//		Iterator<Benchmarkable> it = allQueries.iterator();
//		int i = 0;
//		while (it.hasNext()) {
//			Benchmarkable b = it.next();
//			System.out.println("Benchmark " + (i++) + ":\n" + b.toString());
//		}
//		System.out.println("There were " + i + " queries");
////		
//		Iterator<Query> queries = tpcds.interactiveQueries().iterator();
//		int i = 0;
//		while (queries.hasNext()) {
//			Baggage.discard();
//			XTrace.startTask(true);
//			XTrace.setLoggingLevel(XTraceLoggingLevel.INFO);
//			XTrace.getLogger("Spark Shell").tag("Starting spark shell task", "TPCDS", "Query"+(i+1));
//
//			Query q = queries.next();
//			System.out.println("Query " + i++);
//			System.out.println(q.description());
//			q.doBenchmark(false, "", new ArrayBuffer<String>());
//			
//			Baggage.discard();
//		}
//		val experiment = tpcds.runExperiment(tpcds.interactiveQueries)
//		experiment.waitForFinish(1000 * 60 * 30)
//		
//		experiment.getCurrentRuns().withColumn("result", explode($"results")).select("result.*").groupBy("name").agg(min($"executionTime") as 'minTimeMs,max($"executionTime") as 'maxTimeMs,avg($"executionTime") as 'avgTimeMs,stddev($"executionTime") as 'stdDev).orderBy("name").show(truncate = false)
//		println(s"""Results: sqlContext.read.json("${experiment.resultPath}")""")
//		
//		Baggage.discard()
//		

	}

}
