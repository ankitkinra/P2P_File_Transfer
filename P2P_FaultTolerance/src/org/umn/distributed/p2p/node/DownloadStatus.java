package org.umn.distributed.p2p.node;

import java.util.Map;

import org.umn.distributed.p2p.common.PeerMachine;
import org.umn.distributed.p2p.node.Constants.DOWNLOAD_ACTIVITY;

public class DownloadStatus {
	private String fileToDownload;
	private Map<PeerMachine, DOWNLOAD_ACTIVITY> peersToDownloadFrom;
	private DOWNLOAD_ACTIVITY downloadActivityStatus = null;
	private boolean cancelled = false;
	public String getFileToDownload() {
		return fileToDownload;
	}
	public void setFileToDownload(String fileToDownload) {
		this.fileToDownload = fileToDownload;
	}
	public Map<PeerMachine, DOWNLOAD_ACTIVITY> getPeersToDownloadFrom() {
		return peersToDownloadFrom;
	}
	public void setPeersToDownloadFrom(Map<PeerMachine, DOWNLOAD_ACTIVITY> peersToDownloadFrom) {
		this.peersToDownloadFrom = peersToDownloadFrom;
	}
	public DOWNLOAD_ACTIVITY getDownloadActivityStatus() {
		return downloadActivityStatus;
	}
	public void setDownloadActivityStatus(DOWNLOAD_ACTIVITY downloadActivityStatus) {
		this.downloadActivityStatus = downloadActivityStatus;
	}
	public boolean isCancelled() {
		return cancelled;
	}
	public void setCancelled(boolean cancelled) {
		this.cancelled = cancelled;
	}

	
}
