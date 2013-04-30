package org.umn.distributed.p2p.server;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.umn.distributed.p2p.common.BasicServer;
import org.umn.distributed.p2p.common.LoggingUtils;
import org.umn.distributed.p2p.common.Machine;
import org.umn.distributed.p2p.common.SharedConstants;
import org.umn.distributed.p2p.common.SharedConstants.NODE_REQUEST_TO_SERVER;
import org.umn.distributed.p2p.common.Utils;

public class TrackingServer extends BasicServer {

	protected HashMap<String, HashSet<Machine>> filesServersMap = new HashMap<String, HashSet<Machine>>();
	private ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	protected final Lock readL = rwl.readLock();
	protected final Lock writeL = rwl.writeLock();
	protected boolean removeFailedPeers = false;
	public static final String SERVER_PROPERTIES_FILE_NAME = "server.properties";
	private static boolean testCodeLocal = false;
	private static AtomicBoolean objectCreated = new AtomicBoolean(false);
	private static final String COMMAND_STOP = "stop";

	protected TrackingServer(int port, int numTreads) {
		super(port, numTreads);
		LoggingUtils.logInfo(logger, "objectCreated=%s", objectCreated);
		boolean gotTheLock = objectCreated.compareAndSet(false, true);
		if (!gotTheLock) {
			throw new IllegalStateException("Already one Tracking server created, cannot have more.");
		}
	}

	protected boolean addFile(String fileName, Machine machine) {
		writeL.lock();
		try {
			HashSet<Machine> machinesForArticle = null;
			if (this.filesServersMap.containsKey(fileName)) {
				machinesForArticle = this.filesServersMap.get(fileName);
			} else {
				machinesForArticle = new HashSet<Machine>();
			}
			machinesForArticle.add(machine);
			this.filesServersMap.put(fileName, machinesForArticle);
			return true;// TODO do we need to return False ever
		} finally {
			writeL.unlock();
		}
	}

	/**
	 * TODO Hard problem as this machine is scattered in all of the articles, so
	 * we need to look at every article which is stored
	 * 
	 * @param id
	 * @return
	 */
	protected void removeMachine(Machine m) {
		writeL.lock();
		try {
			for (Entry<String, HashSet<Machine>> entry : this.filesServersMap.entrySet()) {
				entry.getValue().remove(m); // for each article remove the
											// peer
			}
		} finally {
			writeL.unlock();
		}
	}

	protected Set<Machine> getPeersForFile(String fileName) {
		Set<Machine> machineSet = new HashSet<Machine>();
		readL.lock();
		try {
			if (this.filesServersMap.containsKey(fileName)) {
				machineSet.addAll(this.filesServersMap.get(fileName));
			} else {
				logger.warn("tried to find machines for an unknown article =" + fileName);
			}
		} finally {
			readL.unlock();
		}
		return machineSet;
	}

	public int getInternalPort() {
		return this.port;
	}

	@Override
	public void handleServerException(Exception e, String commandReceived) {

		// TODO Auto-generated method stub
		/**
		 * Here we do not need to handle any major things, so we can just log an
		 * error
		 */
		LoggingUtils.logError(logger, e, "Error from the TCPServerHandler");

	}

	/**
	 * <pre>
	 * This method will handle all the request coming into the server
	 * a) (FILE_LIST=<>|MACHINE=[M1])a Node comes with complete list of files.
	 * b) (ADDED_FILE_LIST=<>|MACHINE=[M1])TODO Maybe add a method to handle the delta of files so that network traffic is lessened
	 * FAILED_SERVERS=<list of servers that have failed in the last try to get this file> 
	 * c) (FIND=<filename>|FAILED_SERVERS=[M1][M2][M3]) Node asking for a file's peers
	 * d) OPTIONAL; FAILED_SERVER in the above command is optional feature
	 * </pre>
	 */
	@Override
	protected byte[] handleSpecificRequest(String request) {
		if (!Utils.isEmpty(request)) {
			logger.info("$$$$$$$$$$$$Message received at Tracking Server:" + request);
			if (request.startsWith(NODE_REQUEST_TO_SERVER.FILE_LIST.name())) {
				handleFileUpdateMessage(request);
				return Utils.stringToByte(SharedConstants.COMMAND_SUCCESS);
			} else if (request.startsWith(NODE_REQUEST_TO_SERVER.FIND.name())) {
				String peers = handleFindFileRequest(request);
				return Utils.stringToByte((Utils.isEmpty(peers) ? SharedConstants.COMMAND_FAILED
						: SharedConstants.COMMAND_SUCCESS) + SharedConstants.COMMAND_PARAM_SEPARATOR + peers);

			} else if (request.startsWith(NODE_REQUEST_TO_SERVER.FAILED_PEERS.name())) {
				handleFailedPeerRequest(request);
				return Utils.stringToByte(SharedConstants.COMMAND_SUCCESS);

			}
		}

		return Utils.stringToByte(SharedConstants.INVALID_COMMAND);

	}

	private String handleFindFileRequest(String request) {
		/**
		 * (FIND=<filename>|FAILED_SERVERS=[M1][M2]) Need to find all the peers
		 * serving this file
		 */
		String[] commandFragments = Utils.splitCommandIntoFragments(request);
		// TODO validation here
		String[] filesFromCommandFrag = Utils.getKeyAndValuefromFragment(commandFragments[0]);
		String[] failedPeerList = Utils.getKeyAndValuefromFragment(commandFragments[1], SharedConstants.NO_LIMIT_SPLIT);

		String peers = findPeersForFile(filesFromCommandFrag[1], failedPeerList[1]);
		return peers;
	}

	private void handleFailedPeerRequest(String request) {
		/**
		 * (FAILED_PEERS=[M1][M2]) Need to find all the peers serving this file
		 */
		String[] commandFragments = Utils.getKeyAndValuefromFragment(request);
		if (commandFragments.length > 0) {
			removeFailedPeers(commandFragments[1]);
		}

	}

	private void removeFailedPeers(String failedPeerList) {
		List<Machine> failedPeerCollection = getCollectionFromPeerString(failedPeerList);
		LoggingUtils.logDebug(logger, "failedPeerCollection=%s", failedPeerCollection);
		if (removeFailedPeers) {
			for (Machine m : failedPeerCollection) {
				removeMachine(m);
				LoggingUtils.logDebug(logger, "removed =%s from filePeerMap", m);
			}
		}
	}

	private void handleFileUpdateMessage(String request) {
		/**
		 * FILE_LIST=[f1;f2;f3]|DELETED_LIST=[f4;f5]|MACHINE=[M1] Need to add
		 * all the files from this server to the fileMap
		 */
		String[] commandFragments = Utils.splitCommandIntoFragments(request);
		LoggingUtils.logInfo(logger, "request=%s;;commandFragments=%s;", request, Arrays.toString(commandFragments));
		String[] filesFromCommandFrag = Utils.getKeyAndValuefromFragment(commandFragments[0]);
		String[] deleteFilesFromCommandFrag = Utils.getKeyAndValuefromFragment(commandFragments[1]);
		String[] machineFromCommandFrag = Utils.getKeyAndValuefromFragment(commandFragments[2]);
		addNodeFilesToMap(filesFromCommandFrag[1], Machine.parse(machineFromCommandFrag[1]));
		removeNodeFilesFromMap(deleteFilesFromCommandFrag[1], Machine.parse(machineFromCommandFrag[1]));
		// need to change this to make it consistent with sequential
		// server
	}

	private void removeNodeFilesFromMap(String deletedFiles, Machine sendNode) {
		/**
		 * deletedFiles = [file1;file2;file3]
		 * 
		 */
		String realFileList = deletedFiles.substring(1, deletedFiles.length() - 1);
		LoggingUtils.logInfo(logger, "deletedFiles List after prefix removal=>>%s<<", realFileList);
		String[] deletedFilesArr = Utils.getStringSplitToArr(realFileList, SharedConstants.COMMAND_LIST_SEPARATOR);
		for (String file : deletedFilesArr) {
			removeFile(file, sendNode);
		}
		displayFileServerMap();
	}

	private void removeFile(String fileName, Machine sendNode) {
		writeL.lock();
		try {
			HashSet<Machine> machinesForArticle = null;
			if (this.filesServersMap.containsKey(fileName)) {
				machinesForArticle = this.filesServersMap.get(fileName);
				machinesForArticle.remove(sendNode);
				LoggingUtils.logInfo(logger, "removed peer = %s with file =%s from the fileServersMap", fileName,
						sendNode);
			}
		} finally {
			writeL.unlock();
		}
	}

	/**
	 * accomodating failedPeerList is an optional operation
	 * failedPeerList=[M1][M2][M3]
	 * 
	 * @param fileNameToSearch
	 * @param failedPeerList
	 * @return
	 */
	private String findPeersForFile(String fileNameToSearch, String failedPeerList) {
		StringBuilder peersWithFile = new StringBuilder();
		Set<Machine> peersWithThisFile = getPeersForFile(fileNameToSearch);
		List<Machine> failedPeerCollection = getCollectionFromPeerString(failedPeerList);
		LoggingUtils.logDebug(logger, "peersWithThisFile=%s;failedPeerCollection=%s", peersWithThisFile,
				failedPeerCollection);
		peersWithThisFile.removeAll(failedPeerCollection);

		if (removeFailedPeers) {
			for (Machine m : failedPeerCollection) {
				removeMachine(m);
			}
		}

		for (Machine mc : peersWithThisFile) {
			peersWithFile.append(mc.toString());
		}
		LoggingUtils.logDebug(logger, "peersWithThisFile=%s;failedPeerCollection=%s;peersWithFile=%s",
				peersWithThisFile, failedPeerCollection, peersWithFile.toString());
		return peersWithFile.toString();
	}

	/**
	 * assumption; failedPeerList = [MC1][MC2][MC3]
	 * 
	 * @param failedPeerList
	 * @return
	 */
	private List<Machine> getCollectionFromPeerString(String failedPeerList) {
		return Machine.parseList(failedPeerList);

	}

	private void addNodeFilesToMap(String allFileList, Machine sendNode) {
		/**
		 * Assumption allFileList=[file1;file2;file3]
		 * 
		 */
		String realFileList = allFileList.substring(1, allFileList.length() - 1);
		LoggingUtils.logInfo(logger, "Real File List after prefix removal=>>%s<<", realFileList);
		String[] files = Utils.getStringSplitToArr(realFileList, SharedConstants.COMMAND_LIST_SEPARATOR);
		for (String file : files) {
			addFile(file, sendNode);
		}
		displayFileServerMap();
	}

	private void displayFileServerMap() {
		displayFileServerMap(false);
	}

	private void displayFileServerMap(boolean displayToSystemOut) {
		readL.lock();
		try {
			logger.info(this.filesServersMap);
		} finally {
			readL.unlock();
		}

	}

	public static void showUsage() {
		System.out.println("\n\nUsage:");
		System.out.println("Stop: " + COMMAND_STOP);
	}

	private static void showStartUsage() {
		System.out.println("Usage:");
		System.out.println("\t Start TrackingServer: ./startTrackingServer.sh <config file path> <server-start-port>");
	}

	/**
	 * parameters 1) TrackingServer <property_file> 2) TrackingServer
	 * <property_file> server startPort
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		if (testCodeLocal) {
			testCodeLocally();
		} else {
			TrackingServer ts = null;
			if (args.length != 2) {
				showStartUsage();
				return;
			} else {
				String serverPropertyFileName = args[0];
				ServerProps.loadProperties(serverPropertyFileName);
				int port = 0;
				try {
					port = Integer.parseInt(args[1]);
					if (!Utils.isValidPort(port)) {
						System.out.println("Invalid port");
						showStartUsage();
						return;
					}
				} catch (NumberFormatException nfe) {
					System.out.println("Invalid port");
					showStartUsage();
					return;
				}
				ts = new TrackingServer(port, ServerProps.INTERNAL_SERVER_THREADS);
				try {
					ts.start();
					BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
					boolean stopped = false;
					while (!stopped) {
						showUsage();
						String command = reader.readLine();
						// if (command.startsWith(COMMAND_FIND)) {
						// if (command.length() > COMMAND_FIND.length()) {
						// String fileToFind = command.substring(
						// COMMAND_FIND.length()).trim();
						// if (!Utils.isEmpty(fileToFind)) {
						// List<Machine> machinesWithFile = n
						// .findFileOnTracker(fileToFind, null);
						// List<PeerMachine> avlblPeers = n
						// .getPeerMachineList(machinesWithFile);
						// System.out.println("File " + fileToFind
						// + " found at:");
						// int indx = 1;
						// for (PeerMachine m : avlblPeers) {
						// System.out.print(indx + ". ");
						// System.out.println(m.getString());
						// indx++;
						// }
						// }
						// }
						// } else
						if (command.startsWith(COMMAND_STOP)) {
							stopped = true;
							ts.shutdown();
							System.out.println("Exiting tracking server.");
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("Exiting");
					System.exit(1);
				}
			}
		}
	}

	private static void testCodeLocally() {
		TrackingServer ts = new TrackingServer(Utils.findFreePort(3000), 10);
		Machine[] machineArr = new Machine[2];
		for (int i = 0; i < machineArr.length; i++) {
			machineArr[i] = new Machine("127.0.0.1", 1000 + i);
		}

		try {
			testFileAddition(ts, machineArr);
			testFileSearch(ts);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void testFileAddition(TrackingServer ts, Machine[] machineArr) {
		Random r = new Random();
		int filesToAdd = 0;

		String request = null;
		for (Machine m : machineArr) {
			// for each machine add 3/5 random files
			filesToAdd = r.nextInt(50);
			filesToAdd++; // to make it more than 0
			StringBuilder sb = new StringBuilder("FILE_LIST=[");
			System.out.println("########filesToAdd=" + filesToAdd);
			for (int j = 0; j < filesToAdd; j = j + r.nextInt(2) + 1) {
				sb.append("file_" + j++).append(SharedConstants.COMMAND_LIST_SEPARATOR);
			}
			sb.append("]").append(SharedConstants.COMMAND_PARAM_SEPARATOR).append("MACHINE=").append(m.toString());
			request = sb.toString();
			System.out.println("Node request = " + request);
			byte[] responseBytes = ts.handleSpecificRequest(request);
			ts.displayFileServerMap(true);
			System.out.println(Utils.byteToString(responseBytes));
		}
	}

	private static void testFileSearch(TrackingServer ts) {
		/**
		 * create a list of files to be searched with the help of fileMap and
		 * then manipulate this list
		 * 
		 * <pre>
		 * Aim of this method is to test the request format
		 * (FIND=<filename>|FAILED_SERVERS=[M1][M2])
		 */
		String requestFormat = "FIND=%s|FAILED_SERVERS=%s";
		String finalRequest = null;
		StringBuilder failedPeer = null;
		ts.readL.lock();
		try {
			for (Entry<String, HashSet<Machine>> entry : ts.filesServersMap.entrySet()) {
				failedPeer = new StringBuilder();
				for (Machine m : entry.getValue()) {
					failedPeer.append(m.toString());
					break;
				}
				finalRequest = String.format(requestFormat, entry.getKey(), failedPeer.toString());
				byte[] result = ts.handleSpecificRequest(finalRequest);
				System.out.println("result of find=" + Utils.byteToString(result) + " entrySet=" + entry.getValue());
			}
		} finally {
			ts.readL.unlock();
		}

	}

	@Override
	protected void shutdownSpecific() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void intializeSpecific() {
		// TODO Auto-generated method stub

	}
}
