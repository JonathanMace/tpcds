package edu.brown.cs.systems.tpcds;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;

import com.google.common.io.Files;

/**
 * Invokes the dsdgen platform-specific executable
 */
public class Dsdgen {

	/**
	 * Attempts to extract the dsdgen executable for the current platform to a
	 * temporary directory.  Returns the new, temp directory containing dsdgen.
	 */
	public static File get() {
		// Determine platform
		String platform = null;
		if (SystemUtils.IS_OS_LINUX) platform = "linux";
		else if (SystemUtils.IS_OS_MAC_OSX) platform = "osx";
		if (platform == null) {
			System.err.println("We don't have a precompiled dsdgen binary for platform " + System.getProperty("os.name"));
			return null;
		}
		
		// Extract the resource to a temp file
		File tempDir = Files.createTempDir();
		//tempDir.deleteOnExit();
		try {
			String sourceDirectory = String.format("dsdgen%s%s", File.separator, platform);
			JarUtils.copyDirectory(sourceDirectory, tempDir);
			new File(tempDir, "dsdgen").setExecutable(true);
			return tempDir;
		} catch (IOException e) {
			System.err.println("Failed to extract dsdgen");
			e.printStackTrace();
			return null;
		}
	}
	
	public static BufferedReader invoke(String... args) {
		File dsdgen = get();
		if (dsdgen == null) {
			System.err.println("Unable to invoke dsdgen");
			return null;
		}
		String command = String.format("./dsdgen %s", StringUtils.join(args, " "));
		try {
			ProcessBuilder p = new ProcessBuilder(command.split(" "));
			p.directory(dsdgen);
			return new BufferedReader(new InputStreamReader(p.start().getInputStream()));
		} catch (IOException e) {
			System.err.println("IOException attempt to invoke dsdgen:\n"+command);
			e.printStackTrace();
			return null;
		}
	}
	
	public static void main(String[] args) throws IOException {
		BufferedReader r = invoke(args);
		String line = null;
		while ((line = r.readLine()) != null) {
			System.out.println(line);
		}
	}

}
