package org.umn.distributed.p2p.node;

public class Constants {
	public enum DOWNLOAD_ERRORS {
		FILE_CORRUPT, PEER_UNREACHABLE, OTHER
	};

	public enum DOWNLOAD_ACTIVITY {
		NOT_STARTED, STARTED, DONE, FAILED
	};
}
