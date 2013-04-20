package org.umn.distributed.p2p.node;

import java.util.Map;

import org.umn.distributed.p2p.common.PeerMachine;
import org.umn.distributed.p2p.node.Constants.DOWNLOAD_ACTIVITY;

public class DownloadStatus {
	private String fileToDownload;
	private Map<PeerMachine, DOWNLOAD_ACTIVITY> peersToDownloadFrom;
	private DOWNLOAD_ACTIVITY downloadActivityStatus = null;
	private boolean cancelled = false;

}
