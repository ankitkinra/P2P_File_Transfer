package org.umn.distributed.p2p.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.umn.distributed.p2p.common.SharedConstants;

public class ServerProps {
	private static final String DEFAULT_SERVER_THREAD = "10";

	private static final String DEFAULT_MAX_PSEUDO_NW_DELAY = "1000";

	public static String ENCODING = SharedConstants.DEFAULT_ENCODING;

	public static int WRITER_SERVER_THREADS;
	public static int READER_SERVER_THREADS;

	public static int SERVER_STARTING_PORT = 3000;
	public static int SERVER_EXTERNAL_PORT;
	public static int EXTERNAL_SERVER_THREADS;

	public static int SERVER_INTERNAL_PORT;
	public static int INTERNAL_SERVER_THREADS;

	public static long HEARTBEAT_INTERVAL;
	public static int REMOVE_INTERVAL;

	public static int NETWORK_TIMEOUT;

	public static int maxPseudoNetworkDelay = 1000;
	public static String logFilePath = "log4j.properties";

	public static void loadProperties(String propertyFile) {
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(propertyFile));
			ENCODING = prop.getProperty("encoding", "UTF8");

			// TODO: validate the port numbers before using

			READER_SERVER_THREADS = Integer.parseInt(prop.getProperty("serverWriterThreads", DEFAULT_SERVER_THREAD));
			WRITER_SERVER_THREADS = Integer.parseInt(prop.getProperty("serverReaderThreads", DEFAULT_SERVER_THREAD));

			SERVER_EXTERNAL_PORT = Integer.parseInt(prop.getProperty("serverExternalPort"));
			EXTERNAL_SERVER_THREADS = Integer
					.parseInt(prop.getProperty("externalServerThreads", DEFAULT_SERVER_THREAD));

			SERVER_INTERNAL_PORT = Integer.parseInt(prop.getProperty("serverInternalPort"));
			INTERNAL_SERVER_THREADS = Integer
					.parseInt(prop.getProperty("internalServerThreads", DEFAULT_SERVER_THREAD));

			HEARTBEAT_INTERVAL = Long.parseLong(prop.getProperty("heartbeatInterval"));
			REMOVE_INTERVAL = Integer.parseInt(prop.getProperty("deregisterInterval"));
			NETWORK_TIMEOUT = Integer.parseInt(prop.getProperty("totalNetworkTimeout"));

			maxPseudoNetworkDelay = Integer.parseInt(prop.getProperty("maxPseudoNetworkDelay",
					DEFAULT_MAX_PSEUDO_NW_DELAY));
			logFilePath = prop.getProperty("logFilePath");

		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
}
