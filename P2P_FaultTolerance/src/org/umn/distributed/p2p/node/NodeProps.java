package org.umn.distributed.p2p.node;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.umn.distributed.p2p.common.SharedConstants;

public class NodeProps {
	public static final String DEFAULT_SERVER_THREAD = "10";
	public static String ENCODING = SharedConstants.DEFAULT_ENCODING;
	public static String logFilePath = "log4j.properties";
	public static final long HEARTBEAT_INTERVAL = 10 * 60 * 1000;
	public static final long FAILED_TASK_RETRY_INTERVAL = 10 * 1000;
	public static final int MAX_TASK_TO_RETRY_EVERY_INTERVAL = 5;
	public static final Integer MAX_ATTEMPTS_TO_DOWNLOAD_COURRUPT_FILE = 3;
	public static final int FILE_BUFFER_LENGTH = 1024;
	public static final String DEFAULT_LATENCY_FILE = "latency.properties";
	public static final long UNFINISHED_TASK_INTERVAL = 5 * 1000;
	public static final long UNAVAILABLE_PEER_SERVER_BLOCKED_TIME = 10 * 1000;

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
