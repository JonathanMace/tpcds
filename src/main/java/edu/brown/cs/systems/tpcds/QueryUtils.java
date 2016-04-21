package edu.brown.cs.systems.tpcds;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.brown.cs.systems.tpcds.QueryUtils.Benchmark.Query;

/**
 * Provides utility methods for loading benchmarks and constituent queries from
 * files, which are contained within the jars or files on the classpath
 */
public class QueryUtils {

	static final Logger log = LoggerFactory.getLogger(QueryUtils.class);

	/** Simple representation of a set of queries */
	public static class Benchmark {
		public String benchmarkName;
		public String benchmarkDescription;
		public Map<String, Query> benchmarkQueries = Maps.newHashMap();

		public Benchmark(String name, String description) {
			this.benchmarkName = name;
			this.benchmarkDescription = description;
		}

		public void addQuery(String queryName, String queryText) {
			benchmarkQueries.put(queryName, new Query(queryName, queryText));
		}

		@Override
		public String toString() {
			return String.format("Benchmark %s with %d queries", benchmarkName, benchmarkQueries.size());
		}

		public String toLongString() {
			StringBuilder b = new StringBuilder();
			b.append(this.toString());
			b.append("\n");
			b.append(benchmarkDescription);
			for (Query q : benchmarkQueries.values()) {
				b.append("\n");
				b.append(q.toLongString());
			}
			return b.toString();

		}

		public class Query {
			public String queryName;
			public String queryText;

			public Query(String queryName, String queryText) {
				this.queryName = queryName;
				this.queryText = queryText;
			}
			
			public String toLongString() {
				return String.format("%s/%s:\n\n%s", benchmarkName, queryName, queryText);
			}

			@Override
			public String toString() {
				return String.format("%s/%s", benchmarkName, queryName);
			}
		}
	}

	/**
	 * Finds all available benchmarks and their constituent queries. Looks
	 * inside the 'queries' folder for subfolders. Each folder contains text
	 * files with queries inside. Looks at the README in each folder for a
	 * description of the benchmark.
	 */
	public static Map<String, Benchmark> load() {
		Map<String, Benchmark> allBenchmarks = Maps.newHashMap();
		for (String benchmarkName : availableBenchmarks()) {
			Benchmark benchmark = new Benchmark(benchmarkName, description(benchmarkName));

			// Skip queries that we fail to load
			try {
				List<String> queries = queriesInBenchmark(benchmarkName);
				for (String queryName : queries) {
					try {
						String queryText = loadQuery(benchmarkName, queryName);
						benchmark.addQuery(queryName, queryText);
					} catch (FileNotFoundException e) {
						e.printStackTrace();
						log.warn("Unable to load query " + queryName + " in benchmark " + benchmarkName, e);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
				log.warn("Unable to load queries for benchmark " + benchmarkName, e);
			}

			// Only include benchmarks with at least one query
			if (!benchmark.benchmarkQueries.isEmpty()) {
				allBenchmarks.put(benchmarkName, benchmark);
			}
		}
		return allBenchmarks;
	}

	/**
	 * Multiple different people have implemented TPC-DS queries. This function
	 * returns the names of the variants available in this package. Most queries
	 * don't actually work. View the documentation to see which queries do work
	 * and which ones to use.
	 */
	private static List<String> availableBenchmarks() {
		try {
			return JarUtils.listDir("queries");
		} catch (IOException e) {
			return Lists.<String> newArrayList();
		}
	}

	/** Lists the names of queries in a named benchmark */
	private static List<String> queriesInBenchmark(String benchmark) throws IOException {
		List<String> files = JarUtils.listDir(String.format("queries%s%s", File.separator, benchmark));
		files.remove("README");
		return files;
	}

	/** Returns the benchmark's README text, or the empty string if no text */
	private static String description(String benchmark) {
		String fileName = String.format("queries%s%s%sREADME", File.separator, benchmark, File.separator);
		return JarUtils.readFile(fileName);
	}

	/** Strips comment line and trailing semicolon from query */
	private static String formatQuery(String query) {
		String[] lines = query.split("\n");
		List<String> newLines = Lists.newArrayListWithExpectedSize(lines.length);
		for (String line : lines) {
			// Ignore comment lines
			line = line.trim();
			if (line.startsWith("--")) {
				continue;
			}
			if (line.equals("exit;")) {
				continue;
			}
			while (line.endsWith(";")) {
				line = line.substring(0, line.length() - 1);
			}
			if (!line.isEmpty()) {
				newLines.add(line);
			}
		}
		return StringUtils.join(newLines, "\n");
	}

	/** Loads, from file, a specific query from a benchmark */
	private static String loadQuery(String benchmark, String queryName) throws FileNotFoundException {
		String fileName = String.format("queries%s%s%s%s", File.separator, benchmark, File.separator, queryName);
		return formatQuery(JarUtils.readFile(fileName));
	}

	/** Simple util for printing queries and benchmarks */
	public static void main(String[] args) throws IOException {
		// Get all benchmark data
		Map<String, Benchmark> benchmarks = load();
		
		// No args? Print all available benchmarks
		if (args.length == 0) {
			System.out.println(StringUtils.join(benchmarks.values(), "\n"));
			return;
		}
		
		// Split arg on file separator; either benchmark or query name
		String[] splits = args[0].split(File.separator);
		
		// Get the benchmark, check existence
		Benchmark b = benchmarks.get(splits[0]);
		if (b == null) {
			System.out.println("Unknown benchmark " + splits[0]);
			return;
		}
		
		// No query specified? Print benchmark's query list
		if (splits.length == 1) {
			System.out.println(b);
			System.out.println(b.benchmarkDescription);
			System.out.println(StringUtils.join(b.benchmarkQueries.values(), "\n"));
			return;
		}
		
		// Second arg is query name
		Query q = b.benchmarkQueries.get(splits[1]);
		if (q == null) {
			System.out.printf("Unknown query %s\n", args[0]);
			return;
		}
		
		// Print query
		System.out.println(q.toLongString());
	}

}
