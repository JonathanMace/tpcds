package edu.brown.cs.systems.tpcds.spark;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TPCDSSettings {
	
	public static class SettingsInstance {
		public final int scaleFactor;
		public final String dataLocation;
		public final String dataFormat;
		public final boolean overwrite;
		public final boolean useDoubleForDecimal;
		public final boolean partitionTables;
		public final boolean clusterByPartitionColumns;
		public final boolean filterOutNullPartitionValues;
		public SettingsInstance(Config config) {
			scaleFactor = config.getInt("scaleFactor");
			dataLocation = config.getString("dataLocation");
			dataFormat = config.getString("dataFormat");
			overwrite = config.getBoolean("overwrite");
			useDoubleForDecimal = config.getBoolean("useDoubleForDecimal");
			partitionTables = config.getBoolean("partitionTables");
			clusterByPartitionColumns = config.getBoolean("clusterByPartitionColumns");
			filterOutNullPartitionValues = config.getBoolean("filterOutNullPartitionValues");
		}
	}
	
	private static SettingsInstance defaults = null;
	
	public static SettingsInstance defaults() {
		if (defaults == null) {
			synchronized(TPCDSSettings.class) {
				if (defaults == null) {
					defaults = new SettingsInstance(ConfigFactory.load().getConfig("tpcds"));
				}
			}
		}
		return defaults;
	}
	
	public static int scaleFactor() {
		return defaults().scaleFactor;
	}
	
	public static String dataLocation() {
		return defaults().dataLocation;
	}
	
	public static String dataFormat() {
		return defaults().dataFormat;
	}
	
	public static boolean overwrite() {
		return defaults().overwrite;
	}
	
	public static boolean useDoubleForDecimal() {
		return defaults().useDoubleForDecimal;
	}
	
	public static boolean partitionTables() {
		return defaults().partitionTables;
	}
	
	public static boolean clusterByPartitionColumns() {
		return defaults().clusterByPartitionColumns;
	}
	
	public static boolean filterOutNullPartitionValues() {
		return defaults().filterOutNullPartitionValues;
	}

}
