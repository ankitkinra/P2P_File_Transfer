package org.umn.distributed.p2p.node;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
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
	public DownloadQueueObject(String fileToDownload, Map<PeerMachine, DOWNLOAD_ACTIVITY> mapPeerMachineDownloadStatus, Machine myMachineInfo,
			Queue<DownloadQueueObject> failedTaskQueue, String nodeSpecificOutputFolder,
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
		downloadActivityStatus = DOWNLOAD_ACTIVITY.STARTED;
		for (PeerMachine m : this.peersToDownloadFrom) {
			if (!this.failedMachines.contains(m)) {
				downloadFileFromPeer(this.fileToDownload, m);
			}
		}
		// file is not downloaded yet, declare failed
		if (downloadActivityStatus != DOWNLOAD_ACTIVITY.DONE) {
			downloadActivityStatus = DOWNLOAD_ACTIVITY.FAILED;
		}
	}

	private void downloadFileFromPeer(String fileToDownload2, PeerMachine m) {
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
			fileDownloaded = TCPClient.sendData(m,
					Utils.stringToByte(downloadFileMessage.toString(), NodeProps.ENCODING));

			if (verifyFile(fileDownloaded)) {
				writeFileToFolder(fileDownloaded, this.fileToDownload);
				downloadActivityStatus = DOWNLOAD_ACTIVITY.DONE;
			} else {
				logger.info("failed transfer, data corrupt");
				failedTaskQRef.add(this);
				downloadErrorMap.put(DOWNLOAD_ERRORS.FILE_CORRUPT,
						downloadErrorMap.get(DOWNLOAD_ERRORS.FILE_CORRUPT) + 1);
			}
		} catch (IOException e) {
			// if connection breaks, this means that this peer is down
			LoggingUtils.logError(logger, e, "Error in communicating with tracker server");
			downloadErrorMap.put(DOWNLOAD_ERRORS.PEER_UNREACHABLE,
					downloadErrorMap.get(DOWNLOAD_ERRORS.PEER_UNREACHABLE) + 1);
			this.peerDownloadStatus.put(m, DOWNLOAD_ACTIVITY.FAILED);
			failedMachines.add(m);
			failedTaskQRef.add(this);
		}

	}

	private boolean verifyFile(byte[] fileDownloaded) {
		int number = r.nextInt(100);
		return number < 90; // reducing file fail probability
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
}
