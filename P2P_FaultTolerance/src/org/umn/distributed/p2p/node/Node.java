package org.umn.distributed.p2p.node;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.umn.distributed.p2p.common.BasicServer;
import org.umn.distributed.p2p.common.LoggingUtils;
import org.umn.distributed.p2p.common.Machine;
import org.umn.distributed.p2p.common.PeerMachine;
import org.umn.distributed.p2p.common.SharedConstants;
import org.umn.distributed.p2p.common.SharedConstants.FILES_UPDATE_MESSAGE_TYPE;
import org.umn.distributed.p2p.common.SharedConstants.NODE_REQUEST_TO_NODE;
import org.umn.distributed.p2p.common.TCPClient;
import org.umn.distributed.p2p.common.Utils;

public class Node extends BasicServer {

	protected Logger logger = Logger.getLogger(this.getClass());
	protected int port;
	protected Machine myInfo;
	private final AtomicInteger currentUploads = new AtomicInteger(0);
	private final AtomicInteger currentDownloads = new AtomicInteger(0);
	private int initDownloadQueueCapacity = 10;
	private int initUploadQueueCapacity = 10;
	private DownloadRetryPolicy downloadRetryPolicy = new FileDownloadErrorRetryPolicy();
	private Machine myTrackingServer = null;
	private String directoryToWatchAndSave = null;
	private UpdateTrackingServer updateServerThread = new UpdateTrackingServer();
	private Object updateThreadMonitorObj = new Object();
	/**
	 * A thread needs to monitor the unfinished status queue so that we can
	 * report to the initiator the download status
	 */
	private PriorityQueue<DownloadStatus> unfinishedDownloadStatus = null;

	private Comparator<DownloadQueueObject> downloadPriorityAssignment = null;
	private final DownloadService downloadService = new DownloadService(currentDownloads, downloadRetryPolicy,
			downloadPriorityAssignment, initDownloadQueueCapacity, directoryToWatchAndSave, myInfo);
	private final UploadService uploadService = new UploadService(currentUploads, initUploadQueueCapacity);

	protected Node(int port, int numTreads, Machine trackingServer) {
		super(port, numTreads);
		this.myTrackingServer = trackingServer;
		this.unfinishedDownloadStatus = new PriorityQueue<DownloadStatus>(10,
				DownloadStatus.DOWNLOAD_STATUS_SORTED_BY_UNFINISHED_STATUS);
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleServerException(Exception e, String commandRecieved) {
		// TODO Auto-generated method stub
		LoggingUtils.logError(logger, e, "Error on message=" + commandRecieved);
	}

	/**
	 * <pre>
	 * This method will handle all the request coming into the server
	 * a) GET_LOAD|MACHINE=[M1]
	 * Handle enum NODE_REQUEST_TO_NODE
	 * </pre>
	 */
	@Override
	protected byte[] handleSpecificRequest(String request) {
		if (!Utils.isEmpty(request)) {
			logger.info("$$$$$$$$$$$$Message received at Node:" + request);
			if (request.startsWith(NODE_REQUEST_TO_NODE.GET_LOAD.name())) {
				int load = handleGetLoadMessage(request);
				return Utils.stringToByte(SharedConstants.COMMAND_SUCCESS + "|" + load);
			}
		}

		return Utils.stringToByte(SharedConstants.INVALID_COMMAND);

	}

	private int handleGetLoadMessage(String request) {
		return currentDownloads.get() + currentUploads.get();
	}

	/**
	 * DOWNLOAD_FILE=<filename>|MACHINE=[M1] request to upload this file
	 */
	protected void handleSpecificRequest(String request, OutputStream socketOutput) {
		if (!Utils.isEmpty(request)) {
			logger.info("$$$$$$$$$$$$Message received at Node in special handler:" + request);
			if (request.startsWith(NODE_REQUEST_TO_NODE.DOWNLOAD_FILE.name())) {
				handleUploadFileRequest(request, socketOutput);
			}
		}
	}

	/**
	 * If the file is not found we should write command_failed in the output
	 * stream
	 * 
	 * @param request
	 * @param socketOutput
	 */
	private void handleUploadFileRequest(String request, OutputStream socketOutput) {
		String[] requestArr = Utils.splitCommandIntoFragments(request);
		String[] fileNameArr = Utils.getKeyAndValuefromFragment(requestArr[0]);
		String[] destMachineArr = Utils.getKeyAndValuefromFragment(requestArr[1]);
		Machine m = Machine.parse(destMachineArr[1]);
		uploadService.uploadFile(fileNameArr[1], m, socketOutput);
	}

	/**
	 * <pre>
	 * Sends (FIND=<filename>|FAILED_SERVERS=[M1][M2][M3]) to trackingServer asking for a file's
	 * peers 
	 * ReturnMessage = SUCCESS|[M1][M2][M3] *OR* FAILED
	 * 
	 * @param fileName
	 * @return
	 * @throws IOException this means that the Tracking Sever is down. Need to act by blocking
	 */
	public List<Machine> findFileOnTracker(String fileName, List<PeerMachine> failedPeers) throws IOException {
		List<Machine> foundPeers = null;
		StringBuilder findFileMessage = new StringBuilder(SharedConstants.NODE_REQUEST_TO_SERVER.FIND.name());
		findFileMessage.append(SharedConstants.COMMAND_VALUE_SEPARATOR).append(fileName);
		findFileMessage.append(SharedConstants.COMMAND_PARAM_SEPARATOR).append("FAILED_SERVERS")
				.append(SharedConstants.COMMAND_VALUE_SEPARATOR);
		for (PeerMachine p : failedPeers) {
			findFileMessage.append(p.toString());
		}

		/**
		 * TODO if the server fails this will break and then we need to block on
		 * this
		 */
		byte[] awqReturn = null;
		try {
			awqReturn = TCPClient.sendData(myTrackingServer,
					Utils.stringToByte(findFileMessage.toString(), NodeProps.ENCODING));
		} catch (IOException e) {
			// if connection breaks, it means we need to block on the tracking
			// server
			LoggingUtils.logError(logger, e, "Error in communicating with tracker server");
			throw e;
		}
		String awqStr = Utils.byteToString(awqReturn, NodeProps.ENCODING);
		// return expected as ""
		String[] brokenOnCommandSeparator = awqStr.split(SharedConstants.COMMAND_PARAM_SEPARATOR);
		if (brokenOnCommandSeparator[0].equals(SharedConstants.COMMAND_SUCCESS)) {
			foundPeers = Machine.parseList(brokenOnCommandSeparator[1]);
		}
		return foundPeers;
	}

	/**
	 * (FILE_LIST=<[f1;]>|MACHINE=[M1])a Node comes with complete list of files.
	 * this should also be called from the update the server periodically
	 * 
	 * @param fileName
	 * @param failedPeers
	 * @return
	 * @throws IOException
	 */
	public boolean updateFileList(FILES_UPDATE_MESSAGE_TYPE updateType, List<String> filesToSend) throws IOException {
		StringBuilder updateFileListMessage = new StringBuilder();
		switch (updateType) {
		case COMPLETE:
			updateFileListMessage.append(SharedConstants.NODE_REQUEST_TO_SERVER.FILE_LIST.name());
			break;
		case ADDED:
			updateFileListMessage.append(SharedConstants.NODE_REQUEST_TO_SERVER.ADDED_FILE_LIST.name());
			break;
		}
		/**
		 * FILE_LIST=[f1;f2;f3]|MACHINE=[M1]
		 */
		StringBuilder fileListBuilder = new StringBuilder();
		for (String file : filesToSend) {
			fileListBuilder.append(file).append(SharedConstants.COMMAND_LIST_SEPARATOR);
		}
		updateFileListMessage.append(SharedConstants.COMMAND_VALUE_SEPARATOR).append(fileListBuilder.toString());
		updateFileListMessage.append(SharedConstants.COMMAND_PARAM_SEPARATOR).append("MACHINE")
				.append(SharedConstants.COMMAND_VALUE_SEPARATOR);
		updateFileListMessage.append(myInfo);

		/**
		 * TODO if the server fails this will break and then we need to block on
		 * this
		 */
		byte[] awqReturn = null;
		try {
			awqReturn = TCPClient.sendData(myTrackingServer,
					Utils.stringToByte(updateFileListMessage.toString(), NodeProps.ENCODING));
		} catch (IOException e) {
			LoggingUtils.logError(logger, e, "Error in communicating with tracker server");
			throw e;
		}
		// we will modify the variables sent to us
		String awqStr = Utils.byteToString(awqReturn, NodeProps.ENCODING);

		String[] brokenOnCommandSeparator = awqStr.split(SharedConstants.COMMAND_PARAM_SEPARATOR);
		return brokenOnCommandSeparator[0].equals(SharedConstants.COMMAND_SUCCESS);

	}

	/**
	 * <pre>
	 * adds the download request into the downloadServices queue
	 * We can track the unfinished jobs in the unfinishedDownloadStatus
	 * @param fileName
	 * @return
	 * @throws IOException this means that the Tracking Sever is down. Need to act by blocking
	 */
	public void downloadFileFromPeer(String fileName, List<PeerMachine> avlblPeers) throws IOException {
		DownloadStatus downloadStatus = new DownloadStatus(fileName, avlblPeers);
		this.unfinishedDownloadStatus.add(downloadStatus);
		downloadService.acceptDownloadRequest(downloadStatus);
	}

	private List<String> getFilesFromTheWatchedDirectory(long lastUpdateTime) {
		List<String> listOfFiles = new ArrayList<String>();
		File dir = new File(directoryToWatchAndSave);
		File[] fileListing = dir.listFiles();
		for (File f : fileListing) {
			if (f.lastModified() > lastUpdateTime) {
				listOfFiles.add(f.getName());
			}
		}
		return listOfFiles;
	}

	@Override
	protected void stopSpecific() {
		this.updateServerThread.interrupt();
	}

	@Override
	protected void startSpecific() {
		this.updateServerThread.start();

	}

	private class UpdateTrackingServer extends Thread {
		private long lastUpdateTime = 0;

		@Override
		public void run() {
			try {
				while (true) {
					synchronized (updateThreadMonitorObj) {
						updateThreadMonitorObj.wait(NodeProps.HEARTBEAT_INTERVAL);
					}
					// after wait or notify
					updateFileList(FILES_UPDATE_MESSAGE_TYPE.COMPLETE, getFilesFromTheWatchedDirectory(lastUpdateTime));
					lastUpdateTime = System.currentTimeMillis();
				}
			} catch (Exception e) {
				logger.error("Error in the update server thread", e);
			}

		}
	}

}
