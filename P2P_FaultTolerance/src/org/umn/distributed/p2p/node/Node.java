package org.umn.distributed.p2p.node;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Set;
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
import org.umn.distributed.p2p.node.Constants.DOWNLOAD_ACTIVITY;

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
	private UnfinishedDownloadTaskMonitor unfinishedTaskMonitor = new UnfinishedDownloadTaskMonitor();
	private Object updateThreadMonitorObj = new Object();
	/**
	 * A thread needs to monitor the unfinished status queue so that we can
	 * report to the initiator the download status
	 */
	private PriorityQueue<DownloadStatus> unfinishedDownloadStatus = null;

	private Comparator<DownloadQueueObject> downloadPriorityAssignment = null;
	private final DownloadService downloadService;
	private static HashSet<String> commandsWhoHandleOwnOutput = getCommandsWhoHandleTheirOutput();
	private static Map<LatencyObj, Integer> latencyNumbers = new HashMap<LatencyObj, Integer>();
	private static Set<Integer> latencyMachinesId = new HashSet<Integer>();
	private boolean trackingServerUnavlblBlocked = false;
	private HashMap<String, byte[]> myFilesAndChecksums = new HashMap<String, byte[]>();
	private double avgTimeToServiceUploadRequest = 0.0;
	private long totalUploadRequestHandled = 0;

	private void initLatencyMap() throws IOException {
		if (latencyNumbers.size() == 0) {
			// get the latency file and add the entries

			Properties prop = new Properties();
			try {
				prop.load(new FileInputStream(NodeProps.DEFAULT_LATENCY_FILE));
				for (Entry<Object, Object> entry : prop.entrySet()) {
					List<Machine> machines = Machine.parseList(entry.getKey().toString());
					LatencyObj o = new LatencyObj(machines.get(0), machines.get(1));
					latencyMachinesId.add(machines.get(0).getMachineId());
					latencyNumbers.put(o, Integer.parseInt(entry.getValue().toString()));
					latencyNumbers.put(o.reverseMachines(), Integer.parseInt(entry.getValue().toString()));
				}

			} catch (IOException ex) {
				throw ex;
			}
			LoggingUtils.logInfo(logger, "latencyNumbers=%s after population", latencyNumbers);

		}
	}

	private int getLatency(Machine otherMachine) {
		LatencyObj obj1 = new LatencyObj(this.myInfo, otherMachine);
		if (latencyNumbers.containsKey(obj1)) {
			return latencyNumbers.get(obj1);
		} else if (latencyNumbers.containsKey(obj1.reverseMachines())) {
			return latencyNumbers.get(obj1.reverseMachines());
		} else {
			return Integer.MAX_VALUE;
		}
	}

	protected Node(int port, int numTreads, Machine trackingServer, String dirToWatch, int machineId)
			throws IOException {
		super(port, numTreads, commandsWhoHandleOwnOutput);
		initLatencyMap();
		myInfo.setMachineId(machineId);
		if (!latencyMachinesId.contains(machineId)) {
			String message = String.format("Given machineId=%s does not exists in the Latency file.", machineId);
			LoggingUtils.logInfo(logger, message);
			throw new IllegalArgumentException(message);
		}

		this.myTrackingServer = trackingServer;
		this.directoryToWatchAndSave = appendPortToDir(dirToWatch, this.myInfo.getPort());
		if (directoryToWatchAndSave == null) {
			throw new IllegalArgumentException(
					"Either the directory did not exits or system could not create one. Please check the path and permissions");
		}
		downloadService = new DownloadService(currentDownloads, downloadRetryPolicy, downloadPriorityAssignment,
				initDownloadQueueCapacity, directoryToWatchAndSave, myInfo, updateThreadMonitorObj);
		this.unfinishedDownloadStatus = new PriorityQueue<DownloadStatus>(10,
				DownloadStatus.DOWNLOAD_STATUS_SORTED_BY_UNFINISHED_STATUS);

	}

	private String appendPortToDir(String dirToWatch, int port2) {
		String finalDirectory = dirToWatch + port2 + "\\";
		File f = new File(finalDirectory);
		if (!f.exists()) {
			if (!f.mkdir()) {
				return null;
			}
		}
		return finalDirectory;
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
				StringBuilder returnLoadMessage = new StringBuilder();
				returnLoadMessage.append(SharedConstants.COMMAND_SUCCESS).append(
						SharedConstants.COMMAND_PARAM_SEPARATOR);
				returnLoadMessage.append(load).append(SharedConstants.COMMAND_PARAM_SEPARATOR)
						.append(avgTimeToServiceUploadRequest);
				return Utils.stringToByte(returnLoadMessage.toString());
			} else if (request.startsWith(NODE_REQUEST_TO_NODE.GET_CHECKSUM.name())) {
				byte[] checksum = handleGetChecksumMessage(request);
				return checksum;
			}
		}
		return Utils.stringToByte(SharedConstants.INVALID_COMMAND);

	}

	private byte[] handleGetChecksumMessage(String request) {
		String[] requestArr = Utils.splitCommandIntoFragments(request);
		String[] fileNameArr = Utils.getKeyAndValuefromFragment(requestArr[1]);
		String file = fileNameArr[1];
		return myFilesAndChecksums.get(file);
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
		LoggingUtils.logInfo(logger, "Starting upload number of request=%s; dirToLookIn=%s", request,
				directoryToWatchAndSave);
		String[] requestArr = Utils.splitCommandIntoFragments(request);
		String[] fileNameArr = Utils.getKeyAndValuefromFragment(requestArr[0]);
		String[] destMachineArr = Utils.getKeyAndValuefromFragment(requestArr[1]);
		Machine m = Machine.parse(destMachineArr[1]);
		long timeToUpload = System.currentTimeMillis();
		uploadFile(this.directoryToWatchAndSave + fileNameArr[1], m, socketOutput);
		timeToUpload = System.currentTimeMillis() - timeToUpload;
		updateAverageTimeToServiceUploadRequest(timeToUpload);

	}

	private synchronized double updateAverageTimeToServiceUploadRequest(long timeToServiceRequest) {
		long finishedUploads = totalUploadRequestHandled;
		totalUploadRequestHandled++;
		double denominator = finishedUploads + 1.0;
		avgTimeToServiceUploadRequest = avgTimeToServiceUploadRequest * (finishedUploads / denominator)
				+ timeToServiceRequest / denominator;
		return avgTimeToServiceUploadRequest;
	}

	private void uploadFile(String fileName, Machine machineToSend, OutputStream socketOutput) {
		LoggingUtils.logInfo(logger, "Starting upload of file = %s to peer =%s", fileName, machineToSend);
		currentUploads.incrementAndGet();
		File fileToSend = null;
		try {
			byte[] buffer = new byte[NodeProps.FILE_BUFFER_LENGTH];
			fileToSend = new File(fileName);
			if (fileToSend.isFile() && fileToSend.canRead()) {
				try {
					FileInputStream fileInputStream = new FileInputStream(fileToSend);
					int number = 0;
					LoggingUtils.logInfo(logger, "file = %s to peer =%s;fileToSend = %s", fileName, machineToSend,
							fileToSend.length());

					while ((number = fileInputStream.read(buffer)) >= 0) {

						socketOutput.write(buffer, 0, number);
						socketOutput.flush();

					}
				} catch (FileNotFoundException e) {
					LoggingUtils.logError(logger, e, "File=%s not found and sending error to the peer=%s", fileName,
							machineToSend);
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
			LoggingUtils.logError(logger, e, "Exception while tranferring file File=%s ", fileName);
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
		checkIfBlockedAndAct("findFileOnTracker, findFile=" + fileName);
		StringBuilder findFileMessage = new StringBuilder(SharedConstants.NODE_REQUEST_TO_SERVER.FIND.name());
		findFileMessage.append(SharedConstants.COMMAND_VALUE_SEPARATOR).append(fileName);
		findFileMessage.append(SharedConstants.COMMAND_PARAM_SEPARATOR).append("FAILED_SERVERS")
				.append(SharedConstants.COMMAND_VALUE_SEPARATOR);
		if (failedPeers != null) {
			for (PeerMachine p : failedPeers) {
				findFileMessage.append(p.toString());
			}
		}
		byte[] awqReturn = null;
		try {
			awqReturn = TCPClient.sendData(myTrackingServer, myInfo,
					Utils.stringToByte(findFileMessage.toString(), NodeProps.ENCODING));
			String awqStr = Utils.byteToString(awqReturn, NodeProps.ENCODING);
			// return expected as ""
			String[] brokenOnCommandSeparator = Utils.splitCommandIntoFragments(awqStr);
			if (brokenOnCommandSeparator[0].startsWith(SharedConstants.COMMAND_SUCCESS)) {
				LoggingUtils.logInfo(logger, "peers =%s found for file=%s", brokenOnCommandSeparator[1], fileName);
				foundPeers = Machine.parseList(brokenOnCommandSeparator[1]);
			}
		} catch (IOException e) {
			// if connection breaks, it means we need to block on the tracking
			// server
			LoggingUtils.logError(logger, e, "Error in communicating with tracker server");
			this.trackingServerUnavlblBlock();
			throw e;
		}

		return foundPeers;
	}

	private void calculateAndAddChecksums(List<String> fileNamesToSend) throws Exception {
		for (String fileName : fileNamesToSend) {
			// as the file has come here maybe it has updated modified
			byte[] checkSum = Utils.createChecksum(directoryToWatchAndSave + fileName);
			LoggingUtils.logDebug(logger, "Created checksum =%s for the File =%s", Arrays.toString(checkSum), fileName);
			myFilesAndChecksums.put(fileName, checkSum);
		}

	}

	private void checkIfBlockedAndAct(String message) {
		while (trackingServerUnavlblBlocked) {
			LoggingUtils.logInfo(logger,
					"As tracking server is unavailable we are blocking on this call. Additional message = %s", message);
			try {
				Thread.sleep(NodeProps.UNAVAILABLE_PEER_SERVER_BLOCKED_TIME);
			} catch (InterruptedException e) {
				LoggingUtils.logError(logger, e,
						"Interrupt error while being server unavlbl block, will retry to sleep");
			}
		}
	}

	/**
	 * <pre>
	 * GET_LOAD|MACHINE=[M1]
	 * ReturnMessage = SUCCESS|<loadInt>
	 * 
	 * @throws IOException this means that the Tracking Sever is down. Need to act by blocking
	 */
	public int getLoadFromPeer(Machine peer) throws IOException {
		int load = Integer.MAX_VALUE;
		StringBuilder getLoadMessage = new StringBuilder(SharedConstants.NODE_REQUEST_TO_NODE.GET_LOAD.name());
		getLoadMessage.append(SharedConstants.COMMAND_VALUE_SEPARATOR).append("MACHINE")
				.append(SharedConstants.COMMAND_VALUE_SEPARATOR).append(this.myInfo);

		byte[] awqReturn = null;
		try {
			awqReturn = TCPClient.sendData(peer, myInfo,
					Utils.stringToByte(getLoadMessage.toString(), NodeProps.ENCODING));
			String awqStr = Utils.byteToString(awqReturn, NodeProps.ENCODING);
			String[] brokenOnCommandSeparator = Utils.splitCommandIntoFragments(awqStr);
			if (brokenOnCommandSeparator[0].startsWith(SharedConstants.COMMAND_SUCCESS)) {
				load = Integer.parseInt(brokenOnCommandSeparator[1]);
			}
			return load;
		} catch (IOException e) {
			LoggingUtils.logError(logger, e, "Error in communicating with peer =%s while asking for load", peer);
			// the parent method will handle the removal of this peer from the
			// server
			throw e;
		}

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

		byte[] awqReturn = null;
		try {
			awqReturn = TCPClient.sendData(myTrackingServer, myInfo,
					Utils.stringToByte(updateFileListMessage.toString(), NodeProps.ENCODING));
			String awqStr = Utils.byteToString(awqReturn, NodeProps.ENCODING);
			String[] brokenOnCommandSeparator = awqStr.split(SharedConstants.COMMAND_PARAM_SEPARATOR);
			return brokenOnCommandSeparator[0].equals(SharedConstants.COMMAND_SUCCESS);
		} catch (IOException e) {
			LoggingUtils.logError(logger, e, "Error in communicating with tracker server");
			this.trackingServerUnavlblBlock();
			throw e;
		}

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
		this.unfinishedTaskMonitor.interrupt();
	}

	protected void trackingServerUnavlblBlock() {
		// TODO need to block all the communication with server and stop all
		// functioning
		// Optional still allow peers to talk to each other.
	}

	@Override
	protected void startSpecific() {
		this.updateServerThread.start();
		this.downloadService.start();
		this.unfinishedTaskMonitor.start();
	}

	public static void main(String[] args) {
		// TOOD in the production there would not be localServerIP but one from
		// properties
		System.out.println(Arrays.toString(args));
		String serverIpAddress = args[0];
		String serverPort = args[1];
		int myPort = Integer.parseInt(args[2]);
		Machine trackingServer = new Machine(serverIpAddress, serverPort);
		String dirToWatch = getDirToWatch(args[3]);
		int machineId = Integer.parseInt(args[4]);
		Node n = null;
		try {
			n = new Node(myPort, 10, trackingServer, dirToWatch, machineId);
			n.start();
			System.out.println("Starting node =" + n.myInfo);
			String fileToFind = "big.pdf";
			List<Machine> machinesWithFile = n.findFileOnTracker(fileToFind, null);
			if (machinesWithFile != null) {
				List<PeerMachine> avlblPeers = n.getPeerMachineList(machinesWithFile);
				n.downloadFileFromPeer(fileToFind, avlblPeers);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private List<PeerMachine> getPeerMachineList(List<Machine> machinesWithFile) {
		List<PeerMachine> peers = new LinkedList<PeerMachine>();
		for (Machine m : machinesWithFile) {
			try {
				peers.add(new PeerMachine(m.getIP(), m.getPort(), getLatency(m), getLoadFromPeer(m)));
			} catch (IOException e) {
				LoggingUtils.logError(logger, e, "Did not add peer = %s as it did not return load", m);
				// but we need to ask the server to delete this peer
				List<PeerMachine> failedListToServer = new LinkedList<PeerMachine>();
				failedListToServer.add(new PeerMachine(m.getIP(), m.getPort(), 0, 0));
				try {
					sendFailedPeerMessageToTrackingServer(failedListToServer);
				} catch (IOException e1) {
					LoggingUtils.logError(logger, e, "Could not communicate with Tracking server NEED TO BLOCK", m);
				}
			}
		}
		return peers;
	}

	private static String getDirToWatch(String parentDir) {
		return parentDir;
	}

	private class UpdateTrackingServer extends Thread {
		private long lastUpdateTime = 0;

		@Override
		public void run() {
			try {
				do {
					List<String> fileNamesToSend = getFilesFromTheWatchedDirectory(lastUpdateTime);
					if (fileNamesToSend.size() > 0) {
						calculateAndAddChecksums(fileNamesToSend);
						updateFileList(FILES_UPDATE_MESSAGE_TYPE.COMPLETE, fileNamesToSend);
						if (Node.this.trackingServerUnavlblBlocked) {
							Node.this.trackingServerUnavlblBlocked = false;
						}
					}
					lastUpdateTime = System.currentTimeMillis();
					synchronized (updateThreadMonitorObj) {
						updateThreadMonitorObj.wait(NodeProps.HEARTBEAT_INTERVAL);
					}
				} while (true);
			} catch (Exception e) {
				logger.error("Could not contact TrackigServer from the Update thread need to BLOCk", e);
				Node.this.trackingServerUnavlblBlock();
				/*
				 * this will block anyone else from entering the server but we
				 * still need to poke the tracking server so this command needs
				 * to toggle the tracking server block once we are able to
				 * communicate with it again
				 */
			}

		}

	}

	private class UnfinishedDownloadTaskMonitor extends Thread {
		@Override
		public void run() {
			try {
				while (true) {
					while (!unfinishedDownloadStatus.isEmpty()) {
						DownloadStatus dwnStatus = unfinishedDownloadStatus.peek();
						if (dwnStatus.getDownloadActivityStatus() == DOWNLOAD_ACTIVITY.NOT_STARTED
								|| dwnStatus.getDownloadActivityStatus() == DOWNLOAD_ACTIVITY.STARTED) {
							break;
						}
						// either report or remove
						dwnStatus = unfinishedDownloadStatus.poll();
						if (dwnStatus.getDownloadActivityStatus() == DOWNLOAD_ACTIVITY.DONE) {
							LoggingUtils.logInfo(logger, "Finished download of the task =%s", dwnStatus);
						} else if (dwnStatus.getDownloadActivityStatus() == DOWNLOAD_ACTIVITY.FAILED) {
							LoggingUtils.logInfo(logger, "File=%s could not be downloaded as it is Failed.", dwnStatus);
						} else if (dwnStatus.getDownloadActivityStatus() == DOWNLOAD_ACTIVITY.PEER_UNREACHABLE) {
							LoggingUtils.logInfo(logger,
									"File=%s could not be downloaded as peer unreachable, send to server OPTIONAL.",
									dwnStatus);
							sendServerFailedPeerInfo(dwnStatus);
						}
					}
					try {
						Thread.sleep(NodeProps.UNFINISHED_TASK_INTERVAL);
					} catch (Exception e) {
						logger.error("Error in while loop sleep", e);
					}
				}

			} catch (Exception e) {
				logger.error("Error in the UnfinishedDownloadTaskMonitor", e);
			}

		}

		private void sendServerFailedPeerInfo(DownloadStatus dwnStatus) throws IOException {
			Map<PeerMachine, DOWNLOAD_ACTIVITY> activityStatus = dwnStatus.getPeersToDownloadFrom();
			List<PeerMachine> machinesToReport = new LinkedList<PeerMachine>();
			for (Entry<PeerMachine, DOWNLOAD_ACTIVITY> e : activityStatus.entrySet()) {
				if (e.getValue() == DOWNLOAD_ACTIVITY.PEER_UNREACHABLE) {
					machinesToReport.add(e.getKey());
				}
			}

			sendFailedPeerMessageToTrackingServer(machinesToReport);

		}

	}

	private void sendFailedPeerMessageToTrackingServer(List<PeerMachine> machinesToReport) throws IOException {
		checkIfBlockedAndAct("sendFailedPeerMessageToTrackingServer");
		StringBuilder failedPeersMessage = new StringBuilder(SharedConstants.NODE_REQUEST_TO_SERVER.FAILED_PEERS.name());
		failedPeersMessage.append(SharedConstants.COMMAND_VALUE_SEPARATOR);
		if (machinesToReport != null) {
			for (PeerMachine p : machinesToReport) {
				failedPeersMessage.append(p.toString());
			}
		}

		byte[] awqReturn = null;
		try {
			awqReturn = TCPClient.sendData(myTrackingServer, myInfo,
					Utils.stringToByte(failedPeersMessage.toString(), NodeProps.ENCODING));
			String awqStr = Utils.byteToString(awqReturn, NodeProps.ENCODING);
			// return expected as ""
			String[] brokenOnCommandSeparator = Utils.splitCommandIntoFragments(awqStr);
			if (brokenOnCommandSeparator[0].startsWith(SharedConstants.COMMAND_SUCCESS)) {
				LoggingUtils.logInfo(logger, "Tracking Server updated with the failed peer info");
			}
		} catch (IOException e) {
			LoggingUtils.logError(logger, e, "Error in communicating with tracker server");
			this.trackingServerUnavlblBlock();
			throw e;
		}

	}

	private class LatencyObj {
		private Machine m1 = null;
		private Machine m2 = null;

		public LatencyObj(Machine m1, Machine m2) {
			super();
			this.m1 = m1;
			this.m2 = m2;

		}

		public LatencyObj reverseMachines() {
			return new LatencyObj(this.m2, this.m1);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((m1 == null) ? 0 : m1.hashCode());
			result = prime * result + ((m2 == null) ? 0 : m2.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			LatencyObj other = (LatencyObj) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (m1 == null) {
				if (other.m1 != null)
					return false;
			} else if (!m1.equals(other.m1))
				return false;
			if (m2 == null) {
				if (other.m2 != null)
					return false;
			} else if (!m2.equals(other.m2))
				return false;
			return true;
		}

		private Node getOuterType() {
			return Node.this;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("LatencyObj [m1=");
			builder.append(m1);
			builder.append(", m2=");
			builder.append(m2);
			builder.append("]");
			return builder.toString();
		}

	}
}
