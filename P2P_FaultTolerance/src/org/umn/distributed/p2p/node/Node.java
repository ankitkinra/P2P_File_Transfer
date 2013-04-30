package org.umn.distributed.p2p.node;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.omg.PortableInterceptor.ServerIdHelper;
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
import org.umn.distributed.p2p.node.Constants.PEER_DOWNLOAD_ACTIVITY;

public class Node extends BasicServer {
	protected Logger logger = Logger.getLogger(this.getClass());
	private static final String COMMAND_STOP = "stop";
	private static final String COMMAND_FIND = "find";
	private static final String COMMAND_DOWNLAOD = "download";
	
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
	// private static Map<LatencyObj, Integer> latencyNumbers = new
	// HashMap<LatencyObj, Integer>();
	// private static Set<Integer> latencyMachinesId = new HashSet<Integer>();
	private boolean trackingServerUnavlblBlocked = false;
	private HashMap<String, byte[]> myFilesAndChecksums = new HashMap<String, byte[]>();
	private double avgTimeToServiceUploadRequest = 0.0;
	/*
	 * avgTimeToGetSuccessfulDownload == total time to download a file
	 * successfully (without retry)/total number of files
	 */
	private double avgTimeToGetSuccessfulDownload = 0.0;
	private long totalUploadRequestHandled = 0;
	private long totalDownloadRequested = 0;
	private HashMap<String, HashSet<Machine>> filesServersCache = new HashMap<String, HashSet<Machine>>();
	private boolean testChecksum = false; // used to test the failing of checksum
	/*
	 * private void initLatencyMap() throws IOException { if
	 * (latencyNumbers.size() == 0) { // get the latency file and add the
	 * entries
	 * 
	 * Properties prop = new Properties(); try { prop.load(new
	 * FileInputStream(NodeProps.DEFAULT_LATENCY_FILE)); for (Entry<Object,
	 * Object> entry : prop.entrySet()) { List<Machine> machines =
	 * Machine.parseList(entry.getKey().toString()); LatencyObj o = new
	 * LatencyObj(machines.get(0), machines.get(1));
	 * latencyMachinesId.add(machines.get(0).getMachineId());
	 * latencyNumbers.put(o, Integer.parseInt(entry.getValue().toString()));
	 * latencyNumbers.put(o.reverseMachines(),
	 * Integer.parseInt(entry.getValue().toString())); }
	 * 
	 * } catch (IOException ex) { throw ex; } LoggingUtils.logInfo(logger,
	 * "latencyNumbers=%s after population", latencyNumbers);
	 * 
	 * } }
	 */

	private int getLatency(Machine otherMachine) {
		return LatencyCalculator.calculateLatency(this.myInfo, otherMachine);
		/*
		 * LatencyObj obj1 = new LatencyObj(this.myInfo, otherMachine); if
		 * (latencyNumbers.containsKey(obj1)) { return latencyNumbers.get(obj1);
		 * } else if (latencyNumbers.containsKey(obj1.reverseMachines())) {
		 * return latencyNumbers.get(obj1.reverseMachines()); } else { return
		 * Integer.MAX_VALUE; }
		 */
	}

	public Node(int port, int numTreads, Machine trackingServer, String dirToWatch, int machineId) throws IOException {
		super(port, numTreads, commandsWhoHandleOwnOutput);
		// initLatencyMap();
		myInfo.setMachineId(machineId);
		/*
		 * if (!latencyMachinesId.contains(machineId)) { String message =
		 * String.
		 * format("Given machineId=%s does not exists in the Latency file.",
		 * machineId); LoggingUtils.logInfo(logger, message); throw new
		 * IllegalArgumentException(message); }
		 */

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
		String finalDirectory = dirToWatch + port2 + File.separator;
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

	private synchronized double updateAverageTimeToDownloadFile(long timeToServiceRequest) {
		long finishedDownloads = totalDownloadRequested;
		totalDownloadRequested++;
		double denominator = finishedDownloads + 1.0;
		avgTimeToGetSuccessfulDownload = avgTimeToGetSuccessfulDownload * (finishedDownloads / denominator)
				+ timeToServiceRequest / denominator;
		return avgTimeToGetSuccessfulDownload;
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
			} else {
				// file not found or is unreadable
				LoggingUtils.logInfo(logger, "FileNotFound;file = %s not found hence did not transfer peer =%s",
						fileName, machineToSend);
				socketOutput.write(SharedConstants.FILE_NOT_FOUND);
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
	public List<Machine> findFileOnTracker(String fileName, List<PeerMachine> failedPeers) {
		List<Machine> foundPeers = null;
		if (this.trackingServerUnavlblBlocked) {
			if (NodeProps.enableFileLocationCacheLookup) {
				//
				foundPeers = new LinkedList<Machine>();
				Collection<? extends Machine> existingPeers = getPeersFromCache(fileName);
				if (existingPeers != null) {
					foundPeers.addAll(existingPeers);
				}
				return foundPeers;
			} else {
				checkIfBlockedAndAct("findFileOnTracker, findFile=" + fileName);
			}
		}
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
		//TODO: Kinra check mine changes
		while(true) {
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
				if (foundPeers != null && foundPeers.size() > 0) {
					updateFileServerCache(fileName, foundPeers);
				}
				break;
			} catch (IOException e) {
				// if connection breaks, it means we need to block on the tracking
				// server
				LoggingUtils.logError(logger, e, "Error in communicating with tracker server");
				this.trackingServerUnavlblBlock();
				checkIfBlockedAndAct("findFileOnTracker, findFile=" + fileName);
			}
		}

		return foundPeers;
	}

	private synchronized Collection<? extends Machine> getPeersFromCache(String fileName) {
		return this.filesServersCache.get(fileName);

	}

	private synchronized void updateFileServerCache(String fileName, List<Machine> foundPeers) {
		HashSet<Machine> existingPeersForFile = this.filesServersCache.get(fileName);
		if (existingPeersForFile == null) {
			existingPeersForFile = new HashSet<Machine>();
		}
		for (Machine m : foundPeers) {
			existingPeersForFile.add(m);
		}
		this.filesServersCache.put(fileName, existingPeersForFile);

	}

	private void calculateAndAddChecksums(List<String> fileNamesToSend) throws Exception {
		for (String fileName : fileNamesToSend) {
			// as the file has come here maybe it has updated modified
			byte[] checkSum = Utils.createChecksum(directoryToWatchAndSave + fileName, testChecksum);
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
	public void downloadFileFromPeer(String fileName, List<PeerMachine> avlblPeers) {
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

	public List<String> getFilesFromTheWatchedDirectory(long lastUpdateTime, boolean sendCompleteList) {
		List<String> listOfFiles = new ArrayList<String>();
		File dir = new File(directoryToWatchAndSave);
		File[] fileListing = dir.listFiles();
		for (File f : fileListing) {
			if (sendCompleteList || (f.lastModified() > lastUpdateTime)) {
				listOfFiles.add(f.getName());
			}
		}
		LoggingUtils.logInfo(logger, "Sending fileList = %s from getFilesFromTheWatchedDirectory for peer=%s",
				listOfFiles, myInfo);
		return listOfFiles;
	}

	@Override
	protected void shutdownSpecific() {
		this.updateServerThread.invokeShutdown();
		this.downloadService.stop();
		this.unfinishedTaskMonitor.invokeShutdown();
	}

	protected void trackingServerUnavlblBlock() {
		// TODO need to block all the communication with server and stop all
		// functioning
		// Optional still allow peers to talk to each other.
		this.trackingServerUnavlblBlocked = true;
		LoggingUtils.logInfo(logger,
				"Blocked the peer as Tracking server is unavailable; trackingServerUnavlblBlocked=%s",
				trackingServerUnavlblBlocked);
	}

	@Override
	protected void intializeSpecific() {
		this.updateServerThread.start();
		this.downloadService.start();
		this.unfinishedTaskMonitor.start();
	}

	public static void showStartUsage() {
		System.out.println("Usage:");
		System.out
				.println("Start replica: ./startnode.sh <trackingServerIp> <trackingServerPort> <myPort> <directoryForFiles> <machineId>");
	}

	public static void showUsage() {
		System.out.println("\n\nUsage:");
		System.out.println("Find: " + COMMAND_FIND
				+ " <file name>");
		System.out
				.println("Download: "
						+ COMMAND_DOWNLAOD
						+ " <file name> [<machine list>]");
		System.out.println("eg. ");
		System.out.println(COMMAND_DOWNLAOD + " xyz.txt");
		System.out.println(COMMAND_DOWNLAOD + " abc.txt node1:1000|node2:1000|node2:2000");
		System.out.println("Stop: " + COMMAND_STOP);
	}
	
	public static void main(String[] args) {
		// TOOD in the production there would not be localServerIP but one from
		// properties
		int serverPort = 0;
		int myPort = 0;
		int machineId = 0;
		if (args.length == 5) {
			try {
				serverPort = Integer.parseInt(args[1]);
				myPort = Integer.parseInt(args[2]);
				machineId = Integer.parseInt(args[4]);
				if (!Utils.isValidPort(myPort) || !Utils.isValidPort(serverPort)) {
					System.out.println("Invalid port");
					showStartUsage();
					return;
				}
			} catch (NumberFormatException nfe) {
				System.out.println("Invalid port or machine Id");
				showStartUsage();
				return;
			}
		} else {
			showStartUsage();
			return;
		}
		Machine trackingServer = new Machine(args[0], serverPort);
		String dirToWatch = getDirToWatch(args[3]);
		Node n = null;
		try {
			n = new Node(myPort, 10, trackingServer, dirToWatch, machineId);
			n.start();
			System.out.println("Starting node =" + n.myInfo);
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			boolean stopped = false;
			while (!stopped) {
				showUsage();
				String command = reader.readLine();
				if (command.startsWith(COMMAND_FIND)) {
					if(command.length() > COMMAND_FIND.length()) {
						String fileToFind = command
								.substring(COMMAND_FIND.length()).trim();
						if(!Utils.isEmpty(fileToFind)) {
							List<Machine> machinesWithFile = n.findFileOnTracker(fileToFind, null);
							for (Machine m : machinesWithFile) {
								StringBuilder builder = new StringBuilder();
								builder.append(Machine.FORMAT_START).append(m.getIP()).append(SharedConstants.COMMAND_LIST_SEPARATOR).append(m.getPort())
										.append(Machine.FORMAT_END);
								System.out.println(builder.toString());
							}
						}
					}
				} else if(command.startsWith(COMMAND_DOWNLAOD)) {
					if(command.length() > COMMAND_DOWNLAOD.length()) {
						String fileToFind = command
								.substring(COMMAND_DOWNLAOD.length()).trim();
						if(!Utils.isEmpty(fileToFind)) {
							n.findAndDownloadFile(fileToFind);
						}
					}
				} else if (command.startsWith(COMMAND_STOP)) {
					stopped = true;
					System.out.println("Exiting client.");
				}
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public void findAndDownloadFile(String fileToFind) {
		if (Utils.isNotEmpty(fileToFind)) {
			if (fileNotExistsOnLocal(fileToFind)) {
				List<Machine> machinesWithFile = findFileOnTracker(fileToFind, null);
				if (machinesWithFile != null) {
					List<PeerMachine> avlblPeers = getPeerMachineList(machinesWithFile);
					System.out.println(avlblPeers);
					downloadFileFromPeer(fileToFind, avlblPeers);
					LoggingUtils.logInfo(logger, "Queued the file=%s for download from the following peers=%s",
							fileToFind, avlblPeers);
				}
			} else {
				LoggingUtils.logInfo(logger, "Did not download file=%s as it exists on local", fileToFind);
			}

		} else {
			LoggingUtils.logInfo(logger, "file=%s came as empty", fileToFind);
		}

	}

	private boolean fileNotExistsOnLocal(String fileToFind) {
		return !myFilesAndChecksums.containsKey(fileToFind);
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
		private boolean sendCompleteStatusOnNextAttempt = false;
		private boolean shutdownInvoked = false;

		@Override
		public void run() {

			do {
				try {
					List<String> fileNamesToSend = getFilesFromTheWatchedDirectory(lastUpdateTime,
							sendCompleteStatusOnNextAttempt);
					sendCompleteStatusOnNextAttempt = false;
					if (fileNamesToSend.size() > 0 || trackingServerUnavlblBlocked) {
						calculateAndAddChecksums(fileNamesToSend);
						updateFileList(FILES_UPDATE_MESSAGE_TYPE.COMPLETE, fileNamesToSend);
						if (Node.this.trackingServerUnavlblBlocked) {
							Node.this.trackingServerUnavlblBlocked = false;
							sendCompleteStatusOnNextAttempt = true;
							LoggingUtils.logInfo(logger,
									"Restarted communication with tracking server and hence turned trackingServerUnavlblBlocked=%s."
											+ "Also will send the server the entire information again",
									trackingServerUnavlblBlocked);
						}
					}
					lastUpdateTime = System.currentTimeMillis();
					/**
					 * sendCompleteStatusOnNextAttempt == true, this means that
					 * we want to update the server as soon as possible with all
					 * the file information hence we do not wait the heartbeat
					 * interval. Instead we wait on the object only when we do
					 * not need to update the server completely.
					 */
					if (!sendCompleteStatusOnNextAttempt) {
						try {
							synchronized (updateThreadMonitorObj) {
								updateThreadMonitorObj.wait(NodeProps.HEARTBEAT_INTERVAL);
							}
						} catch (InterruptedException e) {
							if(!shutdownInvoked){
								logger.error("Interrupted Exception caught, stopping updater thread", e);
							}
							
						}
					}
				} catch (Exception e) {
					logger.error("Could not contact TrackigServer from the Update thread need to BLOCk", e);
					Node.this.trackingServerUnavlblBlock();
					/*
					 * this will block anyone else from entering the server but
					 * we still need to poke the tracking server so this command
					 * needs to toggle the tracking server block once we are
					 * able to communicate with it again
					 */
				}
			} while (!shutdownInvoked);

		}

		private void invokeShutdown() {
			this.shutdownInvoked = true;
			this.interrupt();
		}
	}

	/**
	 * TODO When peer_unreachable
	 * 
	 * @author kinra003
	 * 
	 */
	private class UnfinishedDownloadTaskMonitor extends Thread {
		boolean shutdownInvoked = false;

		@Override
		public void run() {
			try {
				/*
				 * TODO Need to determine what happens to the file which has
				 * PEER_UNREACHABLE as it will be retried. We also need to
				 * introduce a counter as to how many times we want that retry
				 * to be done with the same peer list else we can insert the
				 * command as a new one using the download functionality
				 */
				while (!shutdownInvoked) {
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
							// TODO Update the avgTimeToDownloadFile
							if (dwnStatus.getEndTimeOfDownloadFile() != SharedConstants.FILE_FAILED_TIME) {
								// skip this time as the file was not downloaded
								// correctly
								updateAverageTimeToDownloadFile(dwnStatus.getEndTimeOfDownloadFile()
										- dwnStatus.getStartTimeOfDownloadFile());

							}
						} else if (dwnStatus.getDownloadActivityStatus() == DOWNLOAD_ACTIVITY.FAILED) {
							LoggingUtils.logInfo(logger, "Failed download of the task =%s", dwnStatus);
						}
						/*
						 * For the other statuses we might need to remove them
						 * from this queue as they might be retried
						 */
						/*
						 * even if the file was correctly downloaded there might
						 * be some unreachable peers
						 */
						sendServerFailedPeerInfo(dwnStatus);

					}
					try {
						Thread.sleep(NodeProps.UNFINISHED_TASK_INTERVAL);
					} catch (Exception e) {
						if(!shutdownInvoked){
							logger.error("Error in while loop sleep", e);
						}
						// if error while sleeping, this could be interrupt
						// signal, get out
						
					}
				}

			} catch (Exception e) {
				logger.error("Error in the UnfinishedDownloadTaskMonitor", e);
			}

		}

		private void sendServerFailedPeerInfo(DownloadStatus dwnStatus) throws IOException {
			Map<PeerMachine, PEER_DOWNLOAD_ACTIVITY> activityStatus = dwnStatus.getPeersToDownloadFrom();
			List<PeerMachine> machinesToReport = new LinkedList<PeerMachine>();
			for (Entry<PeerMachine, PEER_DOWNLOAD_ACTIVITY> e : activityStatus.entrySet()) {
				if (e.getValue() == PEER_DOWNLOAD_ACTIVITY.UNREACHABLE) {
					machinesToReport.add(e.getKey());
				}
			}

			if (machinesToReport.size() > 0) {
				LoggingUtils.logInfo(logger, "Some peers =%s unreachable, send to server OPTIONAL. \ndwnStatus=%s",
						machinesToReport, dwnStatus);
				sendFailedPeerMessageToTrackingServer(machinesToReport);
			}

		}

		private void invokeShutdown() {
			this.shutdownInvoked = true;
			this.interrupt();
		}

	}

	public boolean hasUnfinishedDownloads() {
		return this.unfinishedDownloadStatus.size() > 0;
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
