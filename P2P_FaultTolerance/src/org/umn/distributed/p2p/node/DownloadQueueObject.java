package org.umn.distributed.p2p.node;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.MaximizeAction;

import org.apache.log4j.Logger;
import org.umn.distributed.p2p.common.LoggingUtils;
import org.umn.distributed.p2p.common.Machine;
import org.umn.distributed.p2p.common.PeerMachine;
import org.umn.distributed.p2p.common.SharedConstants;
import org.umn.distributed.p2p.common.TCPClient;
import org.umn.distributed.p2p.common.Utils;
import org.umn.distributed.p2p.node.Constants.DOWNLOAD_ACTIVITY;
import org.umn.distributed.p2p.node.Constants.DOWNLOAD_ERRORS;

public class DownloadQueueObject implements Runnable {
	private Machine myMachineInfo = null;
	private String fileToDownload;
	private List<PeerMachine> peersToDownloadFrom;
	private DOWNLOAD_ACTIVITY downloadActivityStatus = DOWNLOAD_ACTIVITY.NOT_STARTED;
	private EnumMap<DOWNLOAD_ERRORS, Integer> downloadErrorMap = new EnumMap<Constants.DOWNLOAD_ERRORS, Integer>(
			DOWNLOAD_ERRORS.class);
	private Queue<DownloadQueueObject> failedTaskQRef = null;
	private Logger logger = Logger.getLogger(getClass());
	private String nodeSpecificOutputFolder = null;
	private Random r = new Random();
	private AtomicInteger activeDownloadCount;
	private Map<PeerMachine, DOWNLOAD_ACTIVITY> peerDownloadStatus;

	public String getFileToDownload() {
		return fileToDownload;
	}

	public DOWNLOAD_ACTIVITY getDownloadActivityStatus() {
		return downloadActivityStatus;
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
	public DownloadQueueObject(String fileToDownload, Map<PeerMachine, DOWNLOAD_ACTIVITY> mapPeerMachineDownloadStatus,
			Machine myMachineInfo, Queue<DownloadQueueObject> failedTaskQueue, String nodeSpecificOutputFolder,
			AtomicInteger activeDownloadCount) {
		this.fileToDownload = fileToDownload;
		this.peersToDownloadFrom = convertToList(mapPeerMachineDownloadStatus);
		this.peerDownloadStatus = mapPeerMachineDownloadStatus;
		this.myMachineInfo = myMachineInfo;
		this.failedTaskQRef = failedTaskQueue;
		this.nodeSpecificOutputFolder = nodeSpecificOutputFolder;
		Collections.sort(this.peersToDownloadFrom, PeerMachine.PEER_SELECTION_POLICY);
		this.activeDownloadCount = activeDownloadCount;

		downloadErrorMap.put(DOWNLOAD_ERRORS.FILE_CORRUPT, 0);
		downloadErrorMap.put(DOWNLOAD_ERRORS.OTHER, 0);
		downloadErrorMap.put(DOWNLOAD_ERRORS.PEER_UNREACHABLE, 0);
	}

	private List<PeerMachine> convertToList(Map<PeerMachine, DOWNLOAD_ACTIVITY> mapPeerMachineDownloadStatus) {
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
		LoggingUtils.logInfo(logger, "starting download of file=%s", this.fileToDownload);
		downloadActivityStatus = DOWNLOAD_ACTIVITY.STARTED;
		for (PeerMachine m : this.peersToDownloadFrom) {
			if (!this.failedMachines.contains(m)) {
				downloadFileFromPeer(this.fileToDownload, m);
			}
			if (downloadActivityStatus.equals(DOWNLOAD_ACTIVITY.DONE)) {
				LoggingUtils.logInfo(logger, "file=%s downloaded correctly.", this.fileToDownload);
				break;
			}
		}
		// file is not downloaded yet, declare failed
		if (downloadActivityStatus != DOWNLOAD_ACTIVITY.DONE) {
			LoggingUtils.logInfo(logger, "file=%s cannot be downloaded. The system might retry", this.fileToDownload);
			downloadActivityStatus = DOWNLOAD_ACTIVITY.FAILED;
			failedTaskQRef.add(this); // adding to the retry queue.
		}
	}

	private void downloadFileFromPeer(String fileToDownload2, PeerMachine m) {
		LoggingUtils.logInfo(logger, "starting download of file=%s from peer = %s", this.fileToDownload, m);
		this.peerDownloadStatus.put(m, DOWNLOAD_ACTIVITY.STARTED);
		int activeCount = this.activeDownloadCount.addAndGet(1);
		LoggingUtils.logDebug(logger,
				"Starting the download on the peer = %s for the file =%s and the activeDownloadCount = %s", m,
				fileToDownload2, activeCount);

		StringBuilder downloadFileMessage = new StringBuilder(SharedConstants.NODE_REQUEST_TO_NODE.DOWNLOAD_FILE.name());
		downloadFileMessage.append(SharedConstants.COMMAND_VALUE_SEPARATOR).append(fileToDownload2);
		downloadFileMessage.append(SharedConstants.COMMAND_PARAM_SEPARATOR).append("MACHINE")
				.append(SharedConstants.COMMAND_VALUE_SEPARATOR).append(this.myMachineInfo);
		/**
		 * TODO if the server fails this will break and then we need to block on
		 * this
		 */
		byte[] fileDownloaded = null;
		try {
			int counter = 0;
			while (true) {
				counter++;
				TCPClient.sendDataGetFile(m, Utils.stringToByte(downloadFileMessage.toString(), NodeProps.ENCODING),
						this.nodeSpecificOutputFolder + this.fileToDownload);
				if (verifyFile(fileDownloaded)) {
					// if correct stop and finish
					downloadActivityStatus = DOWNLOAD_ACTIVITY.DONE;
					LoggingUtils.logDebug(logger, "Downloaded file =%s from the peer=%s correctly", fileToDownload2, m);
					break;
				} else {
					// increment the error count
					deleteFile(this.nodeSpecificOutputFolder + this.fileToDownload);
					downloadErrorMap.put(DOWNLOAD_ERRORS.FILE_CORRUPT,
							downloadErrorMap.get(DOWNLOAD_ERRORS.FILE_CORRUPT) + 1);
					LoggingUtils
							.logInfo(
									logger,
									"file = %s failed transfer, as data corrupt.; downloadErrorMap=%s",
									this.fileToDownload, downloadErrorMap);
				}
				if (counter >= NodeProps.MAX_ATTEMPTS_TO_DOWNLOAD_COURRUPT_FILE) {
					LoggingUtils
							.logInfo(
									logger,
									"file = %s failed transfer more than %s on the peer = %s, cannot retry, will add peer to failed list",
									this.fileToDownload, NodeProps.MAX_ATTEMPTS_TO_DOWNLOAD_COURRUPT_FILE, m);
					failedMachines.add(m);
					break;
				}

			}

		} catch (IOException e) {
			// if connection breaks, this means that this peer is down
			LoggingUtils.logError(logger, e, "Error in communicating with peer = " + m);
			downloadErrorMap.put(DOWNLOAD_ERRORS.PEER_UNREACHABLE,
					downloadErrorMap.get(DOWNLOAD_ERRORS.PEER_UNREACHABLE) + 1);
			this.peerDownloadStatus.put(m, DOWNLOAD_ACTIVITY.FAILED);
			LoggingUtils.logInfo(logger, "peer download status= %s;downloadErrorMap=%s", this.peerDownloadStatus,
					this.downloadErrorMap);
			failedMachines.add(m);
		} finally {
			this.activeDownloadCount.decrementAndGet();
		}

	}

	private void deleteFile(String fileName) {
		LoggingUtils.logInfo(logger, "Deleting file =%s as it is bad", fileName);
		File f = new File(fileName);
		f.delete();
	}

	private boolean verifyFile(byte[] fileDownloaded) {
		int number = r.nextInt(100);
		return number < 0; // reducing file fail probability
	}

	private void writeFileToFolder(byte[] fileDownloaded, String fileName) {
		FileOutputStream out = null;
		try {
			out = new FileOutputStream(this.nodeSpecificOutputFolder + fileName);
			out.write(fileDownloaded);
			// TODO trigger the udate-the-server-with-files-thread
		} catch (IOException e) {
			logger.error("IOException while writing the downloaded file", e);
		} finally {
			try {
				out.close();
			} catch (IOException e) {
				logger.error("IOException while writing the downloaded file", e);
			}
		}

	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DownloadQueueObject [myMachineInfo=");
		builder.append(myMachineInfo);
		builder.append(", fileToDownload=");
		builder.append(fileToDownload);
		builder.append(", peersToDownloadFrom=");
		builder.append(peersToDownloadFrom);
		builder.append(", downloadActivityStatus=");
		builder.append(downloadActivityStatus);
		builder.append(", downloadErrorMap=");
		builder.append(downloadErrorMap);
		builder.append(", failedTaskQRef=");
		builder.append(failedTaskQRef);
		builder.append(", nodeSpecificOutputFolder=");
		builder.append(nodeSpecificOutputFolder);
		builder.append(", activeDownloadCount=");
		builder.append(activeDownloadCount);
		builder.append(", peerDownloadStatus=");
		builder.append(peerDownloadStatus);
		builder.append(", failedMachines=");
		builder.append(failedMachines);
		builder.append("]");
		return builder.toString();
	}

}
