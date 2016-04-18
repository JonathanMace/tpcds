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
	
	/** List a directory that may exist within a jar file or on the classpath somewhere
	 * Only valid for resources within this project */
	public static List<String> listDir(String dir) throws IOException {
		// Figure out whether we're loading from a JAR or from file
		File jarFile = new File(JarUtils.class.getProtectionDomain().getCodeSource().getLocation().getPath());

		List<String> contents = Lists.newArrayList();
		if(jarFile.isFile()) {
		    JarFile jar = new JarFile(jarFile);
		    Enumeration<JarEntry> entries = jar.entries(); //gives ALL entries in jar
		    while(entries.hasMoreElements()) {
		    	JarEntry entry = entries.nextElement();
		    	String name = entry.getName();
		        if (name.startsWith(dir + File.separator) && !entry.isDirectory()) {
		        	contents.add(name.substring(dir.length()+1));
		        }
		    }
		    jar.close();
		} else {
			URL benchmarkFolder = Thread.currentThread().getContextClassLoader().getResource(dir);
		    if (benchmarkFolder != null) {
		        try {
		            final File f = new File(benchmarkFolder.toURI());
		            for (File resource : f.listFiles()) {
		            	contents.add(resource.getName());
		            }
		        } catch (URISyntaxException ex) {
		        }
		    }
		}
		return contents;
	}
	
	/** Read a file that may exist within a jar file or on the classpath somewhere
	 * Only valid for resources within this project */
	public static String readFile(String fileName) {
		return new Scanner(Queries.class.getClassLoader().getResourceAsStream(fileName)).useDelimiter("\\Z").next();
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
