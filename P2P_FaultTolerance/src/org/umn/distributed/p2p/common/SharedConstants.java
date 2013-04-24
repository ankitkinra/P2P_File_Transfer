package org.umn.distributed.p2p.common;

public class SharedConstants {
	public static final String COMMAND_PARAM_SEPARATOR = "|";
	public static final String COMMAND_PARAM_SEPARATOR_REGEX = "\\|";
	public static final String COMMAND_VALUE_SEPARATOR = "=";
	public static final String COMMAND_SUCCESS = "SUCCESS";
	public static final String COMMAND_FAILED = "FAILED";
	public static final String INVALID_COMMAND = "INVCOM";
	public static final String COMMAND_LIST_SEPARATOR = ";";
	public static final int NO_LIMIT_SPLIT = -1;

	public static final String DEFAULT_ENCODING = "UTF8";
	public static final String COMMAND_LIST_STARTER = "[";
	public static final String COMMAND_LIST_END = "]";
	public static final int DEFAULT_BUFFER_LENGTH = 1024;

	public static enum NODE_REQUEST_TO_SERVER {
		FILE_LIST, ADDED_FILE_LIST, FIND, FAILED_PEERS;
	};

	public static enum NODE_REQUEST_TO_NODE {
		DOWNLOAD_FILE, GET_LOAD, GET_CHECKSUM;
	};

	public static enum FILES_UPDATE_MESSAGE_TYPE {
		COMPLETE, ADDED
	}
}
