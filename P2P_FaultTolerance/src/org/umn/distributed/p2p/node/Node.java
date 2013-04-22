package org.umn.distributed.p2p.node;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
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
	private final AtomicInteger currentUploads = new AtomicInteger(0);
	private final AtomicInteger currentDownloads = new AtomicInteger(0);
	private int initDownloadQueueCapacity = 10;
	private DownloadRetryPolicy downloadRetryPolicy = new FileDownloadErrorRetryPolicy();
	private Machine myTrackingServer = null;
	private final String directoryToWatchAndSave;
	private UpdateTrackingServer updateServerThread = new UpdateTrackingServer();
	private Object updateThreadMonitorObj = new Object();
	/**
	 * A thread needs to monitor the unfinished status queue so that we can
	 * report to the initiator the download status
	 */
	private PriorityQueue<DownloadStatus> unfinishedDownloadStatus = null;

	private Comparator<DownloadQueueObject> downloadPriorityAssignment = null;
	private final DownloadService downloadService;
	private static HashSet<String> commandsWhoHandleOwnOutput = getCommandsWhoHandleTheirOutput();

	protected Node(int port, int numTreads, Machine trackingServer, String dirToWatch) {
		super(port, numTreads, commandsWhoHandleOwnOutput);
		this.myTrackingServer = trackingServer;
		this.directoryToWatchAndSave = dirToWatch;
		downloadService = new DownloadService(currentDownloads, downloadRetryPolicy, downloadPriorityAssignment,
				initDownloadQueueCapacity, directoryToWatchAndSave, myInfo);
		this.unfinishedDownloadStatus = new PriorityQueue<DownloadStatus>(10,
				DownloadStatus.DOWNLOAD_STATUS_SORTED_BY_UNFINISHED_STATUS);
	}

	private static HashSet<String> getCommandsWhoHandleTheirOutput() {
		HashSet<String> commandsWhoHandleTheirOutput = new HashSet<String>();
		commandsWhoHandleTheirOutput.add(SharedConstants.NODE_REQUEST_TO_NODE.DOWNLOAD_FILE.name());
		return commandsWhoHandleTheirOutput;
	}

	@Override
	public void handleServerException(Exception e, String commandRecieved) {
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
		LoggingUtils.logInfo(logger, "Starting upload of request=%s; dirToLookIn=%s", request, directoryToWatchAndSave);
		String[] requestArr = Utils.splitCommandIntoFragments(request);
		String[] fileNameArr = Utils.getKeyAndValuefromFragment(requestArr[0]);
		String[] destMachineArr = Utils.getKeyAndValuefromFragment(requestArr[1]);
		Machine m = Machine.parse(destMachineArr[1]);

		uploadFile(this.directoryToWatchAndSave + fileNameArr[1], m, socketOutput);
	}

	private void uploadFile(String fileName, Machine machineToSend, OutputStream socketOutput) {
		LoggingUtils.logInfo(logger, "Starting upload of file = %s to peer =%s", fileName, machineToSend);
		currentUploads.incrementAndGet();
		File fileToSend = null;
		try {
			byte[] buffer = new byte[1024];
			fileToSend = new File(fileName);
			if (fileToSend.isFile() && fileToSend.canRead()) {
				try {
					FileInputStream fileInputStream = new FileInputStream(fileToSend);
					int number = 0;
					LoggingUtils.logInfo(logger, "file = %s to peer =%s;fileToSend = %s", fileName, machineToSend,
							fileToSend.length());
					
					while ((number = fileInputStream.read(buffer)) != -1) {
						socketOutput.write(buffer, 0, number);
						socketOutput.flush();
						
					}
				} catch (FileNotFoundException e) {
					LoggingUtils.logError(logger, e, "File=%s not found and sending error to the peer=%s",
							fileName, machineToSend);
					try {
						socketOutput.write(Utils.stringToByte(SharedConstants.COMMAND_FAILED));
					} catch (IOException e1) {
						LoggingUtils.logError(logger, e1,
								"File=%s not found and also could not send error to the peer=%s", fileName,
								machineToSend);
					}
				} catch (IOException e) {
					LoggingUtils.logError(logger, e, "IOException while tranferring file File=%s ", fileName);
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			currentUploads.decrementAndGet();
		}

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
		if (failedPeers != null) {
			for (PeerMachine p : failedPeers) {
				findFileMessage.append(p.toString());
			}
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
		String[] brokenOnCommandSeparator = Utils.splitCommandIntoFragments(awqStr);
		if (brokenOnCommandSeparator[0].startsWith(SharedConstants.COMMAND_SUCCESS)) {
			LoggingUtils.logInfo(logger, "peers =%s found for file=%s", brokenOnCommandSeparator[1], fileName);
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
		StringBuilder fileListBuilder = new StringBuilder(SharedConstants.COMMAND_LIST_STARTER);
		for (String file : filesToSend) {
			fileListBuilder.append(file).append(SharedConstants.COMMAND_LIST_SEPARATOR);
		}
		fileListBuilder.append(SharedConstants.COMMAND_LIST_END);
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
		removeIfMyMachineExistsInPeers(avlblPeers);
		if (avlblPeers.size() > 0) {
			DownloadStatus downloadStatus = new DownloadStatus(fileName, avlblPeers);
			LoggingUtils.logInfo(logger, "download status for file =%s and peers = %s", fileName, avlblPeers);
			this.unfinishedDownloadStatus.add(downloadStatus);
			downloadService.acceptDownloadRequest(downloadStatus);
		} else {
			LoggingUtils.logInfo(logger, "peer list is empty for file = %s", fileName);
		}

	}

	private void removeIfMyMachineExistsInPeers(List<PeerMachine> avlblPeers) {
		Iterator<PeerMachine> itr = avlblPeers.iterator();
		while (itr.hasNext()) {
			PeerMachine iterM = itr.next();
			if (iterM.getIP().equals(myInfo.getIP()) && iterM.getPort() == myInfo.getPort()) {
				itr.remove();
			}
		}

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
		this.downloadService.stop();
	}

	@Override
	protected void startSpecific() {
		this.updateServerThread.start();
		this.downloadService.start();
	}

	public static void main(String[] args) {
		// TOOD in the production there would not be localServerIP but one from
		// properties
		System.out.println(Arrays.toString(args));
		String serverIpAddress = args[0];
		String serverPort = args[1];
		int myPort = Integer.parseInt(args[2]);
		Machine trackingServer = new Machine(serverIpAddress, serverPort);
		String dirToWatch = getDirToWatch(myPort);
		Node n = new Node(myPort, 10, trackingServer, dirToWatch);
		try {
			n.start();
			System.out.println("Starting node =" + n.myInfo);
			String fileToFind = "big.pdf";
			List<Machine> machinesWithFile = n.findFileOnTracker(fileToFind, null);
			if (machinesWithFile != null) {
				List<PeerMachine> avlblPeers = n.getPeerMachineList(machinesWithFile);
				n.downloadFileFromPeer(fileToFind, avlblPeers);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private List<PeerMachine> getPeerMachineList(List<Machine> machinesWithFile) {
		List<PeerMachine> peers = new LinkedList<PeerMachine>();
		for (Machine m : machinesWithFile) {
			peers.add(new PeerMachine(m.getIP(), m.getPort(), 100, 5));
		}
		return peers;
	}

	private static String getDirToWatch(int myPort) {
		return "D:\\p2p\\peers\\" + myPort + "\\";
	}

	private class UpdateTrackingServer extends Thread {
		private long lastUpdateTime = 0;

		@Override
		public void run() {
			try {
				do {

					List<String> fileNamesToSend = getFilesFromTheWatchedDirectory(lastUpdateTime);
					if (fileNamesToSend.size() > 0) {
						updateFileList(FILES_UPDATE_MESSAGE_TYPE.COMPLETE, fileNamesToSend);
					}
					lastUpdateTime = System.currentTimeMillis();
					synchronized (updateThreadMonitorObj) {
						updateThreadMonitorObj.wait(NodeProps.HEARTBEAT_INTERVAL);
					}
				} while (true);
			} catch (Exception e) {
				logger.error("Error in the update server thread", e);
			}

		}
	}

}
