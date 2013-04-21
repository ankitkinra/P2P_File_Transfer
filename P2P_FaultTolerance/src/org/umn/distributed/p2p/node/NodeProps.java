package org.umn.distributed.p2p.node;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.umn.distributed.p2p.common.SharedConstants;

public class NodeProps {
	private static final String DEFAULT_SERVER_THREAD = "10";
	public static String ENCODING = SharedConstants.DEFAULT_ENCODING;
	public static String logFilePath = "log4j.properties";
	public static final long HEARTBEAT_INTERVAL = 5*60*1000;
	public static void loadProperties(String propertyFile) {
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(propertyFile));
			ENCODING = prop.getProperty("encoding", "UTF8");
			logFilePath = prop.getProperty("logFilePath");

		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
}
