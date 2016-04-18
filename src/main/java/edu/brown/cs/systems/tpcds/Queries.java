package edu.brown.cs.systems.tpcds;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;

public class Queries {

	/**
	 * Multiple different people have implemented TPC-DS queries. This function
	 * returns the names of the variants available in this package. Most queries
	 * don't actually work. View the documentation to see which queries do work
	 * and which ones to use.
	 */
	public static List<String> availableBenchmarks() {
		try {
			return JarUtils.listDir("queries");
		} catch (IOException e) {
			return Lists.<String>newArrayList();
		}
	}

	/** Lists the names of queries in a named benchmark 
	 * @throws IOException */
	public static List<String> queriesInBenchmark(String benchmark) throws IOException {
		List<String> files = JarUtils.listDir(String.format("queries%s%s", File.separator, benchmark));
		files.remove("README");
		return files;
	}
	
	/** Returns the benchmark's README text */
	public static String description(String benchmark) {
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

	/**
	 * Finds a named query on the classpath, loads it, and strips out comments
	 * and trailing semicolons
	 */
	public static String loadQuery(String benchmark, String queryName) throws FileNotFoundException {
		String fileName = String.format("queries%s%s%s%s", File.separator, benchmark, File.separator, queryName);
		return formatQuery(JarUtils.readFile(fileName));
	}

	public static void main(String[] args) throws IOException {
		for (String benchmark : availableBenchmarks()) {
			System.out.println(benchmark);
			System.out.println(description(benchmark));
			for (String query : queriesInBenchmark(benchmark)) {
				System.out.println(query);
			}
		}
	}

}
