package org.umn.distributed.p2p.node;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.umn.distributed.p2p.common.SharedConstants;

public class NodeProps {
	public static final String DEFAULT_SERVER_THREAD = "10";
	public static String ENCODING = SharedConstants.DEFAULT_ENCODING;
	public static String logFilePath = "log4j.properties";
	public static boolean enableFileLocationCacheLookup;
	public static double peerSelectionLatencyWeight;
	public static double peerSelectionLoadWeight;
	public static int tcpServerThreads = 10;
	public static long HEARTBEAT_INTERVAL = 1 * 60 * 1000;
	public static long FAILED_TASK_RETRY_INTERVAL = 10 * 1000;
	public static int MAX_TASK_TO_RETRY_EVERY_INTERVAL = 5;
	public static Integer MAX_ATTEMPTS_TO_DOWNLOAD_COURRUPT_FILE = 3;
	public static int FILE_BUFFER_LENGTH = 1024;
	public static String DEFAULT_LATENCY_FILE = "latency.properties";
	public static long UNFINISHED_TASK_INTERVAL = 5 * 1000;
	public static long UNAVAILABLE_PEER_SERVER_BLOCKED_TIME = 10 * 1000;
	public static final String PEER_SELECTION_LATENCY_WEIGHT = "0.01";
	public static final String PEER_SELECTION_LOAD_WEIGHT = "0.1";

	public static void loadProperties(String propertyFile) {
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(propertyFile));
			ENCODING = prop.getProperty("encoding", "UTF8");
			logFilePath = prop.getProperty("logFilePath");
			peerSelectionLatencyWeight = Double.parseDouble(prop.getProperty("peerSelectionLatencyWeight",
					PEER_SELECTION_LATENCY_WEIGHT));
			peerSelectionLoadWeight = Double.parseDouble(prop.getProperty("peerSelectionLoadWeight",
					PEER_SELECTION_LOAD_WEIGHT));
			tcpServerThreads = Integer.parseInt(prop.getProperty("tcpServerThreads", "10"));
			HEARTBEAT_INTERVAL = Long.parseLong(prop.getProperty("heartBeatInterval", "60000"));
			MAX_TASK_TO_RETRY_EVERY_INTERVAL = Integer.parseInt(prop.getProperty("maxTaskToRetry", "10"));
			MAX_ATTEMPTS_TO_DOWNLOAD_COURRUPT_FILE = Integer.parseInt(prop.getProperty(
					"maxAttemptsToDownloadCorruptFile", "3"));
			FILE_BUFFER_LENGTH = Integer.parseInt(prop.getProperty("fileBufferLength", "1024"));
			UNFINISHED_TASK_INTERVAL = Long.parseLong(prop.getProperty("unfinishedTaskInterval", "5000"));
			UNAVAILABLE_PEER_SERVER_BLOCKED_TIME = Long.parseLong(prop.getProperty("unavailablePeerServerBlockedTime",
					"10000"));
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
}
