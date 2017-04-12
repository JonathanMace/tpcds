package edu.brown.cs.systems.tpcds.spark;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
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

public class SparkTPCDSBatchGenerator {

    public static final XTraceLogger xtrace = XTrace.getLogger(SparkTPCDSBatchGenerator.class);
	public static final Logger log = LoggerFactory.getLogger(SparkTPCDSBatchGenerator.class);

	public final String name;
	public final TPCDSSettings settings;
	public final SparkConf sparkConf;
	public final SparkContext sparkContext;
	public final HiveContext sqlContext;
	public final Tables tables;
	
	private SparkTPCDSBatchGenerator(String name, TPCDSSettings settings) {
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
	public static SparkTPCDSBatchGenerator spinUpWithDefaults() {
		return spinUp("SparkTPCDSWorkloadGenerator", TPCDSSettings.createWithDefaults());
	}
	
	/** Load TPC-DS tables into memory sourced using the provided settings */
	public static SparkTPCDSBatchGenerator spinUp(String name, TPCDSSettings settings) {
		return new SparkTPCDSBatchGenerator(name, settings);
	}
	
	public static void main(String[] args) throws FileNotFoundException {
        // Create from default settings
        TPCDSSettings settings = TPCDSSettings.createWithDefaults();

        List<Query> allQueries = new ArrayList<Query>();
		if (args.length == 0) {
		    System.out.println("Attempting all known queries.  To specify queries, provide them as arguments, eg. impala-tpcds-modified-queries/q19.sql");
		    allQueries.addAll(QueryUtils.load().get("impala-tpcds-modified-queries").benchmarkQueries.values());
//		    for (Benchmark benchmark : QueryUtils.load().values()) {
//		        allQueries.addAll(benchmark.benchmarkQueries.values());
//		    }
		} else {
	        for (int i = 0; i < args.length; i++) {
	            String queryName = args[i];
	            
	            // Load the benchmark
	            String[] splits = queryName.split(File.separator);
	            Benchmark b = QueryUtils.load().get(splits[0]);
	            
	            // Bad benchmark
	            if (b == null) {
	                System.out.println("Skipping unknown benchmark " + splits[0]);
	                continue;
	            }
	            
	            // No query specified
	            if (splits.length <= 1) {
	                System.out.println("No query specified, expected dataset and query, eg impala-tpcds-modified-queries/q19.sql");
	                continue;
	            }   
	        
	            // Get the query
	            Query q = b.benchmarkQueries.get(splits[1]);
	            
	            // Bad query
	            if (q == null) {
	                System.out.println("Skipping unknown query " + queryName);
	            }

	            System.out.printf("Will attempt query %s\n", q);
	        }
		}

        System.out.printf("Will attempt %d queries on %s dataset %s\n", allQueries.size(), settings.dataFormat, settings.dataLocation);
		
        
        System.out.println("Loading tables into memory...");
        long preLoad = System.currentTimeMillis();        
		SparkTPCDSBatchGenerator gen = spinUp("SparkTPCDSWorkloadGenerator", settings);
        long postLoad = System.currentTimeMillis();
		System.out.printf("Loading tables into memory took %.1f seconds\n", (postLoad - preLoad) / 1000.0);

		String outputFileName = "batch_" + postLoad + ".log";
		PrintWriter statusLog = new PrintWriter(outputFileName);
		String[] headers = { "t", "i", "benchmark", "query", "benchmark.query", "duration", "successful", "errorreason", "taskid", "auxtaskid" };
		statusLog.println(StringUtils.join(headers, "\t"));
		
		System.out.println("Running " + allQueries.size() + " queries, writing output to " + outputFileName);
		
		int iteration = 1;
		Long taskId = null;
		for (Query query : allQueries) {
    		Baggage.discard();
    		if (taskId == null) {
    		    XTrace.startTask(true);
    		    taskId = XTraceBaggageInterface.getTaskID();
    		} else {
    		    // Set a higher task ID so that previous task gets all events if there's a conflict -- this is useful for checking where instrumentation is wrong
    		    taskId += 10;
    		    XTrace.setTask(taskId, 0L);
    		}
    		Retro.setTenant(3);
    		Retro.enableInBaggageCounting(true);
            Netro.set("query", query.queryName);
    		
    		xtrace.tag("Running TPCDS query", query.queryName, "TPCDS");
    
    		// Run the query
    		long begin = System.currentTimeMillis();
    		long end;
    		boolean successful = false;
    		String errorreason = "";
    		try {
    		    System.out.println("Running " + query);
    		    Row[] rows = gen.sqlContext.sql(query.queryText).collect();
    		    end = System.currentTimeMillis();
    		    successful = true;
    		    System.out.printf("%s completed successfully in %.1f seconds\n", query, (end-begin) / 1000.0);
    		    xtrace.log(String.format("Completed in %.1f seconds", (end-begin)/1000.0), "Baggage", edu.brown.cs.systems.tracingplane.transit_layer.Baggage.take());
    		} catch (Exception e) {
    		    end = System.currentTimeMillis();
    		    System.out.println("Query " + query + " failed due to " + e.getClass().getSimpleName() + ": " + e.getMessage());
    		    errorreason = e.getClass().getSimpleName();
    		}
            Object[] row = { end, iteration, query.benchmarkName(), query.queryName, query, end-begin, successful, errorreason, toHexString(taskId), toHexString(taskId+1) };
            statusLog.println(StringUtils.join(row, "\t"));
            statusLog.flush();
		    
		    iteration++;
		}
		
		statusLog.close();
	}
	
	public static String toHexString(long value) {
	    return String.format("%16s", Long.toHexString(value)).replace(' ', '0');
	}

}
