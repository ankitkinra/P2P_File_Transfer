package org.umn.distributed.p2p.server;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.umn.distributed.p2p.common.LoggingUtils;
import org.umn.distributed.p2p.common.Machine;
import org.umn.distributed.p2p.common.SharedConstants;
import org.umn.distributed.p2p.common.SharedConstants.NODE_REQUEST_TO_SERVER;
import org.umn.distributed.p2p.common.Utils;

public class TrackingServer implements TcpServerDelegate {

	protected Logger logger = Logger.getLogger(this.getClass());
	private TCPServer tcpServer;
	protected int port;
	protected Machine myInfo;
	protected HashMap<String, HashSet<Machine>> filesServersMap = new HashMap<String, HashSet<Machine>>();
	private ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	protected final Lock readL = rwl.readLock();
	protected final Lock writeL = rwl.writeLock();

	protected TrackingServer(int port, int numTreads) {
		this.port = port;
		this.tcpServer = new TCPServer(this, numTreads);
	}

	public void start() throws Exception {
		logger.info("****************Starting Server****************");
		try {
			this.port = this.tcpServer.startListening(this.port);
			myInfo = new Machine(Utils.getLocalServerIp(), this.port);
			startSpecific();
		} catch (IOException ioe) {
			logger.error("Error starting tcp server. Stopping now", ioe);
			this.stop();
			throw ioe;
		}
	}

	public void startSpecific() {
		// TODO
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
			for (Entry<String, HashSet<Machine>> entry : this.filesServersMap
					.entrySet()) {
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
				logger.warn("tried to find machines for an unknown article ="
						+ fileName);
			}
		} finally {
			readL.unlock();
		}
		return machineSet;
	}

	public int getInternalPort() {
		return this.port;
	}

	public void stop() {
		this.tcpServer.stop();
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
	public byte[] handleRequest(byte[] request) {
		try {
			String req = Utils.byteToString(request);
			return handleSpecificRequest(req);
		} catch (Exception e) {
			logger.error("Exception handling request in Tracking Server", e);
			return Utils.stringToByte(SharedConstants.COMMAND_FAILED
					+ SharedConstants.COMMAND_PARAM_SEPARATOR + e.getMessage());
		}

	}

	private byte[] handleSpecificRequest(String request) {
		if (!Utils.isEmpty(request)) {
			String[] reqBrokenOnCommandParamSeparator = request
					.split(SharedConstants.COMMAND_PARAM_SEPARATOR);
			logger.info("$$$$$$$$$$$$Message received at Tracking Server:"
					+ reqBrokenOnCommandParamSeparator);
			if (request.startsWith(NODE_REQUEST_TO_SERVER.FILE_LIST.name())) {
				/**
				 * (FILE_LIST=[f1;f2;f3]|MACHINE=[M1]) Need to add all the files
				 * from this server to the fileMap
				 */
				String[] commandFragments = Utils
						.splitCommandIntoFragments(request);
				LoggingUtils.logDebug(logger,
						"request=%s;;commandFragments=%s;", request,
						Arrays.toString(commandFragments));
				// TODO validation here
				String[] filesFromCommandFrag = Utils
						.getKeyAndValuefromFragment(commandFragments[0]);
				String[] machineFromCommandFrag = Utils
						.getKeyAndValuefromFragment(commandFragments[1]);
				addNodeFilesToMap(filesFromCommandFrag[1],
						Machine.parse(machineFromCommandFrag[1]));
				// need to change this to make it consistent with sequential
				// server
				return Utils.stringToByte(SharedConstants.COMMAND_SUCCESS);
			} else if (request.startsWith(NODE_REQUEST_TO_SERVER.FIND.name())) {
				/**
				 * (FIND=<filename>|FAILED_SERVERS=[M1][M2]) Need to find all
				 * the peers serving this file
				 */
				String[] commandFragments = Utils
						.splitCommandIntoFragments(request);
				// TODO validation here
				String[] filesFromCommandFrag = Utils
						.getKeyAndValuefromFragment(commandFragments[0]);
				String[] failedPeerList = Utils
						.getKeyAndValuefromFragment(commandFragments[1]);

				String peers = findPeersForFile(filesFromCommandFrag[1],
						failedPeerList[1]);
				// need to change this to make it consistent with sequential
				// server
				return Utils.stringToByte(SharedConstants.COMMAND_SUCCESS
						+ SharedConstants.COMMAND_PARAM_SEPARATOR + peers);
			}
		}

		return Utils.stringToByte(SharedConstants.INVALID_COMMAND);

	}

	/**
	 * accomodating failedPeerList is an optional operation
	 * failedPeerList=[M1][M2][M3]
	 * 
	 * @param fileNameToSearch
	 * @param failedPeerList
	 * @return
	 */
	private String findPeersForFile(String fileNameToSearch,
			String failedPeerList) {
		StringBuilder peersWithFile = new StringBuilder();
		Set<Machine> peersWithThisFile = getPeersForFile(fileNameToSearch);
		List<Machine> failedPeerCollection = getCollectionFromPeerString(failedPeerList);
		peersWithThisFile.removeAll(failedPeerCollection);
		for (Machine m : failedPeerCollection) {
			removeMachine(m);
		}
		for (Machine mc : peersWithThisFile) {
			peersWithFile.append(mc.toString());
		}
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
		String realFileList = allFileList
				.substring(1, allFileList.length() - 1);
		LoggingUtils.logDebug(logger,
				"Real File List after prefix removal=>>%s<<", realFileList);
		String[] files = Utils.getStringSplitToArr(realFileList,
				SharedConstants.COMMAND_LIST_SEPARATOR);
		for (String file : files) {
			addFile(file, sendNode);
		}
	}

	public static void main(String[] args) {
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
				sb.append("file_" + j++).append(
						SharedConstants.COMMAND_LIST_SEPARATOR);
			}
			sb.append("]").append(SharedConstants.COMMAND_PARAM_SEPARATOR)
					.append("MACHINE=").append(m.toString());
			request = sb.toString();
			System.out.println("Node request = " + request);
			byte[] responseBytes = ts.handleSpecificRequest(request);
			System.out.println("filesServersMap=" + ts.filesServersMap);
			System.out.println(Utils.byteToString(responseBytes));
		}
	}
	
	private static void testFileSearch(TrackingServer ts) {
		
	}
}
