package org.umn.distributed.p2p.node;

public class Constants {
	public enum DOWNLOAD_ERRORS {
		FILE_CORRUPT, PEER_UNREACHABLE
	};

	public enum DOWNLOAD_ACTIVITY {
		/**
		 * FAILED == failure due to other than ALL_PEERS_UNREACHABLE
		 * 
		 */
		NOT_STARTED, STARTED, DONE, FAILED, ALL_PEERS_UNREACHABLE
	};

	public enum PEER_DOWNLOAD_ACTIVITY {
		/**
		 * Download can fail due to FILE_NOT_FOUND or FILE_CORRUPT or
		 * UNREACHABLE
		 */
		NOT_STARTED, STARTED, DONE, FILE_CORRUPT, FILE_NOT_FOUND, UNREACHABLE
	};
}
