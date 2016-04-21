package edu.brown.cs.systems.tpcds;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.Scanner;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.commons.io.IOUtils;

import com.google.common.collect.Lists;

public class JarUtils {
	
	public static List<String> listFilesystemDir(String dir) {
		URL benchmarkFolder = Thread.currentThread().getContextClassLoader().getResource(dir);
		List<String> contents = Lists.newArrayList();
	    if (benchmarkFolder != null) {
	        try {
	            final File f = new File(benchmarkFolder.toURI());
	            for (File resource : f.listFiles()) {
	            	contents.add(resource.getName());
	            }
	        } catch (URISyntaxException ex) {
	        }
	    }
	    return contents;
	}
	
	/** Lists the contents of a directory that resides within a jarfile */
	public static List<String> listDirFromJarFile(JarFile jar, String dir) {
	    Enumeration<JarEntry> entries = jar.entries(); //gives ALL entries in jar
	    List<String> directoryEntries = Lists.newArrayList();
	    while(entries.hasMoreElements()) {
	    	JarEntry entry = entries.nextElement();
	    	String name = entry.getName();
	    	
	    	// Ignore anything that isn't actually in the dir
	    	if (!name.startsWith(dir)) {
	    		continue;
	    	}
	    	
	    	// Ignore the dir itself, only interested in dir contents
	    	if (name.equals(dir)) {
	    		continue;
	    	}
	    	
	    	// Get the name of this path element in the dir
	    	String nameInDir = name.substring(dir.length()).split(File.separator)[0];
	    	
	    	// Ignore the directory entry if we've seen it before
	    	if (directoryEntries.contains(nameInDir)) {
	    		continue;
	    	}
	    	
	    	// Append to list of entries
	    	directoryEntries.add(nameInDir);
	    }
	    return directoryEntries;
	}
	
	/** List a directory that may exist within a jar file or on the classpath somewhere
	 * Only valid for resources within this project */
	public static List<String> listDir(String dir) throws IOException {
		// Figure out whether we're loading from a JAR or from file
		File jarFile = new File(JarUtils.class.getProtectionDomain().getCodeSource().getLocation().getPath());
		if(jarFile.isFile()) {
			JarFile jar = new JarFile(jarFile);
			try {
				return listDirFromJarFile(jar, dir+File.separator);
			} finally {
				jar.close();
			}
		} else {
			return listFilesystemDir(dir);
		}
	}
	
	/** Read a file that may exist within a jar file or on the classpath somewhere
	 * Only valid for resources within this project */
	public static String readFile(String fileName) {
		InputStream rsrc = QueryUtils.class.getClassLoader().getResourceAsStream(fileName);
		String fileContents = "";
		if (rsrc != null) {
			Scanner s = new Scanner(rsrc);
			s.useDelimiter("\\Z"); // EOF delimiter
			if (s.hasNext()) {
				fileContents = s.next();
			}
			s.close();
		}
		return fileContents;
	}
	
	public static void copyDirectory(String resourceDirName, File destDir) throws IOException {
		for (String file : JarUtils.listDir(resourceDirName)) {
			String resourceFileName = File.separator + resourceDirName + File.separator + file;
			File destFile = new File(destDir, file);
			destFile.createNewFile();
			destFile.deleteOnExit();
			copyResource(resourceFileName, destFile);
		}
	}
	
	public static void copyResource(String resourceFileName, File destFile) throws IOException {
		OutputStream out = new FileOutputStream(destFile);
		InputStream in = Dsdgen.class.getResourceAsStream(resourceFileName);
		try {
			IOUtils.copy(in, out);
		} finally {
			in.close();
			out.close();
		}
	}
	
}
