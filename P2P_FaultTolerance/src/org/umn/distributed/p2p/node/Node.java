package org.umn.distributed.p2p.node;

import java.io.IOException;
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
import org.umn.distributed.p2p.common.TCPClient;
import org.umn.distributed.p2p.common.TCPServer;
import org.umn.distributed.p2p.common.Utils;

public class Node extends BasicServer {

	protected Logger logger = Logger.getLogger(this.getClass());
	private TCPServer tcpServer;
	protected int port;
	protected Machine myInfo;
	private final AtomicInteger currentUploads = new AtomicInteger(0);
	private final AtomicInteger currentDownloads = new AtomicInteger(0);
	private int initDownloadQueueCapacity = 10;
	private DownloadRetryPolicy downloadRetryPolicy = null;
	private Machine myTrackingServer = null;
	/**
	 * A thread needs to monitor the unfinished status queue so that we can
	 * report to the initiator the download status
	 */
	private PriorityQueue<DownloadStatus> unfinishedDownloadStatus = null;

	private Comparator<DownloadQueueObject> downloadPriorityAssignment = null;
	private final DownloadService downloadService = new DownloadService(currentDownloads, downloadRetryPolicy,
			downloadPriorityAssignment, initDownloadQueueCapacity);

	protected Node(int port, int numTreads, Machine trackingServer) {
		super(port, numTreads);
		this.myTrackingServer = trackingServer;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleServerException(Exception e) {
		// TODO Auto-generated method stub

	}

	/**
	 * <pre>
	 * This method will handle all the request coming into the server
	 * a) DOWNLOAD_FILE=<filename>|MACHINE=[M1] request to upload this file
	 * b) GET_LOAD|MACHINE=[M1]
	 * Handle enum NODE_REQUEST_TO_NODE
	 * </pre>
	 */
	@Override
	protected byte[] handleSpecificRequest(String message) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * <pre>
	 * (FIND=<filename>|FAILED_SERVERS=[M1][M2][M3]) Node asking for a file's
	 * peers 
	 * ReturnMessage = SUCCESS|[M1][M2][M3] *OR* FAILED
	 * 
	 * @param fileName
	 * @return
	 * @throws IOException this means that the Tracking Sever is down. Need to act by blocking
	 */
	private List<Machine> findFileOnTracker(String fileName, List<PeerMachine> failedPeers) throws IOException {
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
		// we will modify the variables sent to us
		String awqStr = Utils.byteToString(awqReturn, NodeProps.ENCODING);
		// return expected as ""
		String[] brokenOnCommandSeparator = awqStr.split(SharedConstants.COMMAND_PARAM_SEPARATOR);
		if (brokenOnCommandSeparator[0].equals(SharedConstants.COMMAND_SUCCESS)) {
			foundPeers = Machine.parseList(brokenOnCommandSeparator[1]);
		}
		return foundPeers;
	}
	
	/**
	 * (FILE_LIST=<>|MACHINE=[M1])a Node comes with complete list of files.
	 * @param fileName
	 * @param failedPeers
	 * @return
	 * @throws IOException
	 */
	private boolean updateFileList(FILES_UPDATE_MESSAGE_TYPE updateType, List<String> filesToSend) throws IOException {
		StringBuilder updateFileListMessage = new StringBuilder();
		switch (updateType){
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
		for(String file:filesToSend){
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

	@Override
	protected void stopSpecific() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void startSpecific() {
		// TODO Auto-generated method stub

	}

}
