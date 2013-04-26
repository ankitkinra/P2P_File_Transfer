package org.umn.distributed.p2p.node;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.umn.distributed.p2p.common.PeerMachine;
import org.umn.distributed.p2p.node.Constants.DOWNLOAD_ACTIVITY;
import org.umn.distributed.p2p.node.Constants.PEER_DOWNLOAD_ACTIVITY;

public class DownloadStatus {
	private static final AtomicInteger downloadIdGenerator = new AtomicInteger(1);
	private int downloadId = downloadIdGenerator.getAndIncrement();
	private String fileToDownload;
	private Map<PeerMachine, PEER_DOWNLOAD_ACTIVITY> peersToDownloadFrom;
	private DOWNLOAD_ACTIVITY downloadActivityStatus = DOWNLOAD_ACTIVITY.NOT_STARTED;
	private boolean cancelled = false;
	private long startTimeOfDownloadFile = 0;
	private long endTimeOfDownloadFile = 0;

	public DownloadStatus(String fileToDownload, List<PeerMachine> peersToDownloadFrom) {
		super();
		this.fileToDownload = fileToDownload;
		Map<PeerMachine, PEER_DOWNLOAD_ACTIVITY> mapPeerActivity = new HashMap<PeerMachine, PEER_DOWNLOAD_ACTIVITY>();
		for (PeerMachine p : peersToDownloadFrom) {
			mapPeerActivity.put(p, PEER_DOWNLOAD_ACTIVITY.NOT_STARTED);
		}
		this.peersToDownloadFrom = mapPeerActivity;
		this.startTimeOfDownloadFile = System.currentTimeMillis();
	}

	public String getFileToDownload() {
		return fileToDownload;
	}

	public void setFileToDownload(String fileToDownload) {
		this.fileToDownload = fileToDownload;
	}

	public Map<PeerMachine, PEER_DOWNLOAD_ACTIVITY> getPeersToDownloadFrom() {
		return peersToDownloadFrom;
	}

	public void setPeersToDownloadFrom(Map<PeerMachine, PEER_DOWNLOAD_ACTIVITY> peersToDownloadFrom) {
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

	public long getEndTimeOfDownloadFile() {
		return endTimeOfDownloadFile;
	}

	public void setEndTimeOfDownloadFile(long endTimeOfDownloadFile) {
		this.endTimeOfDownloadFile = endTimeOfDownloadFile;
	}

	public int getDownloadId() {
		return downloadId;
	}

	public long getStartTimeOfDownloadFile() {
		return startTimeOfDownloadFile;
	}

	public static final Comparator<DownloadStatus> DOWNLOAD_STATUS_SORTED_BY_UNFINISHED_STATUS = new Comparator<DownloadStatus>() {

		@Override
		public int compare(DownloadStatus o1, DownloadStatus o2) {
			int statusComparison = o1.downloadActivityStatus.compareTo(o2.downloadActivityStatus);
			if (statusComparison != 0) {
				return -1 * statusComparison;
			} else {
				// downloadId of one cannot be equal to another, definite order
				return o1.downloadId > o2.downloadId ? 1 : -1;
			}
		}
	};

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DownloadStatus [downloadId=");
		builder.append(downloadId);
		builder.append(", fileToDownload=");
		builder.append(fileToDownload);
		builder.append(", peersToDownloadFrom=");
		builder.append(peersToDownloadFrom);
		builder.append(", downloadActivityStatus=");
		builder.append(downloadActivityStatus);
		builder.append(", cancelled=");
		builder.append(cancelled);
		builder.append("]");
		return builder.toString();
	}

}
