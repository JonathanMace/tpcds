package edu.brown.cs.systems.tpcds.spark;

import java.io.File;
import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databricks.spark.sql.perf.tpcds.Tables;
import edu.brown.cs.systems.baggage.Baggage;
import edu.brown.cs.systems.retro.Netro;
import edu.brown.cs.systems.retro.Retro;
import edu.brown.cs.systems.tpcds.QueryUtils;
import edu.brown.cs.systems.tpcds.QueryUtils.Benchmark;
import edu.brown.cs.systems.tpcds.QueryUtils.Benchmark.Query;
import edu.brown.cs.systems.xtrace.XTrace;
import edu.brown.cs.systems.xtrace.XTraceBaggageInterface;
import edu.brown.cs.systems.xtrace.logging.XTraceLogger;

public class SparkTPCDSWorkloadGenerator {

    public static final XTraceLogger xtrace = XTrace.getLogger(SparkTPCDSWorkloadGenerator.class);
	public static final Logger log = LoggerFactory.getLogger(SparkTPCDSWorkloadGenerator.class);

	public final String name;
	public final TPCDSSettings settings;
	public final SparkConf sparkConf;
	public final SparkContext sparkContext;
	public final HiveContext sqlContext;
	public final Tables tables;
	
	private SparkTPCDSWorkloadGenerator(String name, TPCDSSettings settings) {
		this.name = name;
		this.settings = settings;
		this.sparkConf = new SparkConf().setAppName(name);
		this.sparkContext = new SparkContext(sparkConf);
		this.sqlContext = new HiveContext(sparkContext);
		
		// Load the tables into memory using the spark-sql-perf Tables code
		this.tables = new Tables(sqlContext, settings.scaleFactor);
		tables.createTemporaryTables(settings.dataLocation, settings.dataFormat, "");
	}
	
	/** Load TPC-DS tables into memory using default configuration */
	public static SparkTPCDSWorkloadGenerator spinUpWithDefaults() {
		return spinUp("SparkTPCDSWorkloadGenerator", TPCDSSettings.createWithDefaults());
	}
	
	/** Load TPC-DS tables into memory sourced using the provided settings */
	public static SparkTPCDSWorkloadGenerator spinUp(String name, TPCDSSettings settings) {
		return new SparkTPCDSWorkloadGenerator(name, settings);
	}
	
	
	public static void main(String[] args) throws FileNotFoundException {
		if (args.length != 1) {
			System.out.println("Expected argument specifying dataset and query, eg impala-tpcds-modified-queries/q19.sql");
			return;
		}

		long preLoad = System.currentTimeMillis();

		// Load the benchmark
		String[] splits = args[0].split(File.separator);
		Benchmark b = QueryUtils.load().get(splits[0]);
		
		// Bad benchmark
		if (b == null) {
			System.out.println("Unknown benchmark " + splits[0]);
			return;
		}
		
		// No query specified
		if (splits.length <= 1) {
			System.out.println("No query specified, expected dataset and query, eg impala-tpcds-modified-queries/q19.sql");
			return;
		}
		
		// Get the query
		Query q = b.benchmarkQueries.get(splits[1]);
		
		// Bad query
		if (q == null) {
			System.out.println("Unknown query " + args[0]);
			return;
		}
		
		// Create from default settings
		TPCDSSettings settings = TPCDSSettings.createWithDefaults();
		System.out.printf("Running query %s on %s dataset %s\n", q, settings.dataFormat, settings.dataLocation);
		SparkTPCDSWorkloadGenerator gen = spinUp("SparkTPCDSWorkloadGenerator", settings);

		
		int numQueries = 3;
		Long taskId = null;
		for (int i = 0; i < numQueries; i++) {
	        long postLoad = System.currentTimeMillis();
    		Baggage.discard();
    		if (taskId == null) {
    		    XTrace.startTask(true);
    		    taskId = XTraceBaggageInterface.getTaskID();
    		} else {
    		    // Set a higher task ID so that previous task gets all events if there's a conflict -- this is useful for checking where instrumentation is wrong
    		    taskId += 10;
    		    XTrace.setTask(taskId, 0L);
    		}
    		Retro.setTenant(i);
    		Retro.enableInBaggageCounting(true);
    		
    		String qNumStr = splits[1].replace(".sql", "").replace("q","");
            System.out.printf("Setting baggage to use query %s", qNumStr);
            Netro.set("query", qNumStr);
    		
    		xtrace.tag("Running TPCDS query", q.queryName, "TPCDS");
    
    		// Run the query
    		Row[] rows = gen.sqlContext.sql(q.queryText).collect();
    		
    		// Print the output rows
    		for (Row r : rows) {
    			System.out.println(r);
    		}
    
    		long postQ = System.currentTimeMillis();
    		System.out.printf("Load time: %d, Query time: %d\n", postLoad-preLoad, postQ-postLoad);
    		xtrace.log(String.format("Load time: %d, Query time: %d", postLoad-preLoad, postQ-postLoad));
		}
	}

}
