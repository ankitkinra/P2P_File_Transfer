package org.umn.distributed.p2p.node;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.umn.distributed.p2p.common.LoggingUtils;
import org.umn.distributed.p2p.common.Machine;
import org.umn.distributed.p2p.common.PeerMachine;
import org.umn.distributed.p2p.common.SharedConstants;
import org.umn.distributed.p2p.common.TCPClient;
import org.umn.distributed.p2p.common.Utils;
import org.umn.distributed.p2p.node.Constants.DOWNLOAD_ACTIVITY;
import org.umn.distributed.p2p.node.Constants.DOWNLOAD_ERRORS;
import org.umn.distributed.p2p.node.Constants.PEER_DOWNLOAD_ACTIVITY;

public class DownloadQueueObject implements Runnable {
	private Machine myMachineInfo = null;
	private List<PeerMachine> peersToDownloadFrom;
	// private DOWNLOAD_ACTIVITY downloadActivityStatus =
	// DOWNLOAD_ACTIVITY.NOT_STARTED;
	private EnumMap<DOWNLOAD_ERRORS, Integer> downloadErrorMap = new EnumMap<Constants.DOWNLOAD_ERRORS, Integer>(
			DOWNLOAD_ERRORS.class);
	private Queue<DownloadQueueObject> failedTaskQRef = null;
	private Logger logger = Logger.getLogger(getClass());
	private String nodeSpecificOutputFolder = null;
	private AtomicInteger activeDownloadCount;
	private Map<PeerMachine, PEER_DOWNLOAD_ACTIVITY> peerDownloadStatus;
	private Object updateThreadMonitorObj;
	private DownloadStatus dwnldStatus;

	public String getFileToDownload() {
		return this.dwnldStatus.getFileToDownload();
	}

	public EnumMap<DOWNLOAD_ERRORS, Integer> getDownloadErrorMap() {
		return downloadErrorMap;
	}

	/**
	 * 1) task is to determine from which peer should we start downloading 1 a)
	 * This is done by looking at the PeerMachine and sorting them according to
	 * the sort order PeerMachine.peerSelectionPolicy, once we have them order,
	 * we do
	 * 
	 * @param activeDownloadCount
	 */
	public DownloadQueueObject(DownloadStatus dwnldStatus, Machine myMachineInfo,
			Queue<DownloadQueueObject> failedTaskQueue, String nodeSpecificOutputFolder,
			AtomicInteger activeDownloadCount, Object updateThreadMonitorObj) {
		this.peersToDownloadFrom = convertToList(dwnldStatus.getPeersToDownloadFrom());
		this.peerDownloadStatus = dwnldStatus.getPeersToDownloadFrom();
		this.dwnldStatus = dwnldStatus;
		this.myMachineInfo = myMachineInfo;
		this.failedTaskQRef = failedTaskQueue;
		this.nodeSpecificOutputFolder = nodeSpecificOutputFolder;
		Collections.sort(this.peersToDownloadFrom, PeerMachine.PEER_SELECTION_POLICY);
		this.activeDownloadCount = activeDownloadCount;
		this.updateThreadMonitorObj = updateThreadMonitorObj;

		downloadErrorMap.put(DOWNLOAD_ERRORS.FILE_CORRUPT, 0);
		downloadErrorMap.put(DOWNLOAD_ERRORS.PEER_UNREACHABLE, 0);
	}

	private List<PeerMachine> convertToList(Map<PeerMachine, PEER_DOWNLOAD_ACTIVITY> mapPeerMachineDownloadStatus) {
		List<PeerMachine> peers = new ArrayList<PeerMachine>();
		for (PeerMachine m : mapPeerMachineDownloadStatus.keySet()) {
			peers.add(m);
		}
		return peers;
	}

	/**
	 * On each failure we enter in this Set all the failed machines And on the
	 * next trial we do not take machine from this set even if they are good in
	 * the latency numbers
	 */
	private HashSet<PeerMachine> failedMachines = new HashSet<PeerMachine>();

	/**
	 * <pre>
	 * 1. pick the first the host from the peer download list and if it is not in the failedMachines
	 * use that else, keep on going down the list, and as soon as we hit usable peer we start the download
	 * using the TCPClient call to ask the server for the download.
	 * 2. if we run into an error we need to insert this task back in failedTaskQRef
	 */
	public void run() {
		LoggingUtils.logInfo(logger, "starting download of file=%s", this.dwnldStatus.getFileToDownload());
		this.dwnldStatus.setDownloadActivityStatus(DOWNLOAD_ACTIVITY.STARTED);
		try {
			this.failedMachines.clear();
			for (PeerMachine m : this.peersToDownloadFrom) {
				if (!this.failedMachines.contains(m)) {
					try {
						downloadFileFromPeer(this.dwnldStatus.getFileToDownload(), m);
					} catch (Exception e) {
						// An exception here means that the peer was unreachable
						// now,
						// even after the load stage it responded
						this.failedMachines.add(m);
					}
				}
				if (this.dwnldStatus.getDownloadActivityStatus().equals(DOWNLOAD_ACTIVITY.DONE)) {
					this.dwnldStatus.setEndTimeOfDownloadFile(System.currentTimeMillis());
					synchronized (this.updateThreadMonitorObj) {
						this.updateThreadMonitorObj.notifyAll();
					}
					LoggingUtils.logInfo(logger, "file=%s downloaded correctly.", this.dwnldStatus.getFileToDownload());
					break;
				}
			}
			if (!this.dwnldStatus.getDownloadActivityStatus().equals(DOWNLOAD_ACTIVITY.DONE)) {
				if (this.failedMachines.size() == this.peersToDownloadFrom.size()) {
					LoggingUtils.logInfo(logger, "All peers failed, will retry");
					this.dwnldStatus.setDownloadActivityStatus(DOWNLOAD_ACTIVITY.ALL_PEERS_UNREACHABLE);
					this.dwnldStatus.setEndTimeOfDownloadFile(SharedConstants.FILE_FAILED_TIME);
					failedTaskQRef.add(this);
				} else {
					/*
					 * download did not complete, some peer might be reachable
					 * but may not have the file or the file might have exceeded
					 * download tries Either ways we would not like to retry the
					 * download in this situation
					 */
					this.dwnldStatus.setDownloadActivityStatus(DOWNLOAD_ACTIVITY.FAILED);
					this.dwnldStatus.setEndTimeOfDownloadFile(SharedConstants.FILE_FAILED_TIME);
				}

			}
		} finally {
			// no exception needs to be caught
			this.activeDownloadCount.decrementAndGet();
		}
	}

	/**
	 * @param fileToDownload2
	 * @param m
	 * @throws Exception
	 */
	private void downloadFileFromPeer(String fileToDownload2, PeerMachine m) throws Exception {
		LoggingUtils.logInfo(logger, "starting download of file=%s from peer = %s",
				this.dwnldStatus.getFileToDownload(), m);
		this.peerDownloadStatus.put(m, PEER_DOWNLOAD_ACTIVITY.STARTED);
		int activeCount = this.activeDownloadCount.get();
		LoggingUtils.logDebug(logger,
				"Starting the download on the peer = %s for the file =%s and the activeDownloadCount = %s", m,
				fileToDownload2, activeCount);

		StringBuilder downloadFileMessage = new StringBuilder(SharedConstants.NODE_REQUEST_TO_NODE.DOWNLOAD_FILE.name());
		downloadFileMessage.append(SharedConstants.COMMAND_VALUE_SEPARATOR).append(fileToDownload2);
		downloadFileMessage.append(SharedConstants.COMMAND_PARAM_SEPARATOR).append("MACHINE")
				.append(SharedConstants.COMMAND_VALUE_SEPARATOR).append(this.myMachineInfo);

		byte[] downloadedFileChecksum = null;
		String fileToDownloadAt = this.nodeSpecificOutputFolder + this.dwnldStatus.getFileToDownload();
		try {
			int counter = 0;
			while (true) {
				counter++;
				downloadedFileChecksum = TCPClient.sendDataGetFile(m, myMachineInfo,
						Utils.stringToByte(downloadFileMessage.toString(), NodeProps.ENCODING), fileToDownloadAt);
				if (verifyFile(this.dwnldStatus.getFileToDownload(), m, downloadedFileChecksum, fileToDownloadAt)) {
					// if correct stop and finish
					this.dwnldStatus.setDownloadActivityStatus(DOWNLOAD_ACTIVITY.DONE);
					LoggingUtils.logDebug(logger, "Downloaded file =%s from the peer=%s correctly", fileToDownload2, m);
					break;
				} else {
					// increment the error count
					deleteFile(fileToDownloadAt);
					downloadErrorMap.put(DOWNLOAD_ERRORS.FILE_CORRUPT,
							downloadErrorMap.get(DOWNLOAD_ERRORS.FILE_CORRUPT) + 1);
					LoggingUtils.logInfo(logger, "file = %s failed transfer, as data corrupt.; downloadErrorMap=%s",
							this.dwnldStatus.getFileToDownload(), downloadErrorMap);
				}
				if (counter >= NodeProps.MAX_ATTEMPTS_TO_DOWNLOAD_COURRUPT_FILE) {
					LoggingUtils
							.logInfo(
									logger,
									"file = %s failed transfer more than %s on the peer = %s, cannot retry, will add peer to failed list",
									this.dwnldStatus.getFileToDownload(),
									NodeProps.MAX_ATTEMPTS_TO_DOWNLOAD_COURRUPT_FILE, m);
					this.peerDownloadStatus.put(m, PEER_DOWNLOAD_ACTIVITY.UNREACHABLE);
					// failedMachines.add(m); // Not adding the corrupted
					// download to the failed list, just move on
					break;
				}

			}

		} catch (FileNotFoundException fe) {
			LoggingUtils.logError(logger, fe, "File =%s was not found on the peer=%s, maybe update the own cache ",
					fileToDownload2, m);
			deleteFile(fileToDownloadAt);
		} catch (IOException e) {
			// if connection breaks, this means that this peer is down
			LoggingUtils.logError(logger, e, "Error in communicating with peer = " + m);
			downloadErrorMap.put(DOWNLOAD_ERRORS.PEER_UNREACHABLE,
					downloadErrorMap.get(DOWNLOAD_ERRORS.PEER_UNREACHABLE) + 1);
			this.peerDownloadStatus.put(m, PEER_DOWNLOAD_ACTIVITY.UNREACHABLE);
			LoggingUtils.logInfo(logger, "peer download status= %s;downloadErrorMap=%s", this.peerDownloadStatus,
					this.downloadErrorMap);
			failedMachines.add(m);
		}

	}

	private void deleteFile(String fileName) {
		LoggingUtils.logInfo(logger, "Deleting file =%s as it is bad", fileName);
		File f = new File(fileName);
		f.delete();
	}

	private boolean verifyFile(String filenameDownloaded, Machine peerDownloadedFrom, byte[] fileDownloadedChecksum,
			String fileToDownloadAt) throws Exception {
		LoggingUtils.logDebug(logger,
				"verification of downloaded file begins. fileName=%s, machine =%s fileDownloadedChecksum=%s",
				filenameDownloaded, peerDownloadedFrom, fileDownloadedChecksum);
		if (fileDownloadedChecksum == null) {
			return false;
		}
		StringBuilder downloadFileMessage = new StringBuilder(SharedConstants.NODE_REQUEST_TO_NODE.GET_CHECKSUM.name());
		downloadFileMessage.append(SharedConstants.COMMAND_PARAM_SEPARATOR).append("FILE")
				.append(SharedConstants.COMMAND_VALUE_SEPARATOR).append(filenameDownloaded);
		byte[] checksum = null;
		try {
			checksum = TCPClient.sendData(peerDownloadedFrom, myMachineInfo,
					Utils.stringToByte(downloadFileMessage.toString(), NodeProps.ENCODING));
		} catch (IOException e) {
			LoggingUtils.logError(logger, e, "Error in communicating with peer =%s while asking for checksum",
					peerDownloadedFrom);
		}
		LoggingUtils.logDebug(logger, "fileDownloadedChecksum=%s; checksum=%s",
				Arrays.toString(fileDownloadedChecksum), Arrays.toString(checksum));
		if (checksum == null || fileDownloadedChecksum == null) {
			return false;
		} else {
			return Arrays.equals(fileDownloadedChecksum, checksum);
		}

	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DownloadQueueObject [myMachineInfo=");
		builder.append(myMachineInfo);
		builder.append(", fileToDownload=");
		builder.append(this.dwnldStatus.getFileToDownload());
		builder.append(", peersToDownloadFrom=");
		builder.append(peersToDownloadFrom);
		builder.append(", downloadActivityStatus=");
		builder.append(this.dwnldStatus.getDownloadActivityStatus());
		builder.append(", downloadErrorMap=");
		builder.append(downloadErrorMap);
		builder.append(", failedTaskQRef=");
		builder.append(failedTaskQRef);
		builder.append(", nodeSpecificOutputFolder=");
		builder.append(nodeSpecificOutputFolder);
		builder.append(", activeDownloadCount=");
		builder.append(activeDownloadCount.get());
		builder.append(", peerDownloadStatus=");
		builder.append(peerDownloadStatus);
		builder.append(", failedMachines=");
		builder.append(failedMachines);
		builder.append("]");
		return builder.toString();
	}

}
