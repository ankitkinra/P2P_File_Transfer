package org.umn.distributed.p2p.testfrmwrk;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import org.umn.distributed.p2p.common.LoggingUtils;
import org.umn.distributed.p2p.common.Machine;
import org.umn.distributed.p2p.common.Utils;
import org.umn.distributed.p2p.node.Node;
import org.umn.distributed.p2p.node.NodeProps;

import com.thoughtworks.xstream.XStream;

/**
 * <pre>
 * This testClient will launch several peers and then also keep their info in a structure
 * It will then start firing commands to these nodes using the node object which is saved
 * It will then ask the server for the all the available files (extra method to enable testing)
 * Once it gets these files then it will pick files at random and start the testing according to
 * the xml file
 * </pre>
 * 
 * @author akinra
 * 
 */
public class TestClient {
	private static final String GET_FILE_LIST_COMMAND = "getFileList";
	private static final String LAUNCH_PEERS_COMMAND = "launchPeers";
	private static final String DOWNLOAD_FILE_COMMAND = "downloadFile";
	private static final String DOWNLOAD_FILE_RANDOM_COMMAND = "downloadFileRandom";
	private static final long SLEEP_TIME_PEER_SHUTDOWN = 1000;

	private String xmlTestCasePath = null;
	private Logger logger = Logger.getLogger(TestClient.class);
	private TestSuite testSuite = null;
	private Machine myTrackingServer;
	private HashMap<Integer, RoundSummary> roundSummaries = new HashMap<Integer, RoundSummary>();
	private Random randomGenerator = new Random();
	private int nodeStartPort = 0;
	private HashMap<Integer, Node> idNodeMap = new HashMap<Integer, Node>();
	private HashMap<Integer, List<String>> nodeidFilesMap = new HashMap<Integer, List<String>>();

	public TestClient(String xmlFilePath, String trackingIp, int trackingPort, int nodeStartPort) {
		this.xmlTestCasePath = xmlFilePath;
		this.testSuite = getParsedTestSuite(xmlFilePath);
		this.myTrackingServer = new Machine(trackingIp, trackingPort);
		this.nodeStartPort = nodeStartPort;
	}

	public static void showUsage() {
		System.out.println("Usage:");
		System.out.println("Run tests: ./runtests.sh <Test Config File> <Tracking Ip> <Tracking Port> "
				+ "<Node Start Port> <config file path>");
	}

	public static void main(String[] args) {
		if (args.length == 4) {
			String xmlTestFile = args[0];
			String trackingServerIP = args[1];
			try {
				int trackingServerPort = Integer.parseInt(args[2]);
				if (!Utils.isValidPort(trackingServerPort)) {
					System.out.println("Invalid port for trackingServerPort=" + trackingServerPort);
					showUsage();
					return;
				}
				int nodeStartPort = Integer.parseInt(args[3]);
				if (!Utils.isValidPort(nodeStartPort)) {
					System.out.println("Invalid port for nodeStartPort=" + nodeStartPort);
					showUsage();
					return;
				}
				TestClient tc = new TestClient(xmlTestFile, trackingServerIP, trackingServerPort, nodeStartPort);
				System.out.println(tc.testSuite);
				tc.startTest();
			} catch (NumberFormatException nfe) {
				System.out.println("Invalid port");
				showUsage();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			showUsage();
		}
	}

	private void startTest() throws InterruptedException {
		for (int i = 0; i < this.testSuite.rounds.size(); i++) {
			// init new summary
			Round r = this.testSuite.rounds.get(i);
			RoundSummary rs = new RoundSummary(r);
			this.roundSummaries.put(i, rs);
			logger.info("In the testClient starting round =" + r.name);
			int numberOfPeerstoLaunch = 0;
			for (Operation op : r.operations) {
				/**
				 * The first operation of any Test Case should be launching of
				 * the peers and the after the launch we need to ensure that all
				 * the TODO need to create a method in the peers which can give
				 * the test suite all the files which are local. can we check if
				 * the files have reached the server?
				 * 
				 */
				if (op.name.equals(LAUNCH_PEERS_COMMAND)) {
					if (idNodeMap.size() > 0) {
						throw new IllegalArgumentException("Cannot have launchPeers more than once in a test file");
					}
					String numberOfPeerstoLaunchStr = op.params.get("numberOfPeers");
					String numberThreadsStr = op.params.get("numThreads");
					String dirToWatch = op.params.get("dirToWatch");
					int numberThreads = 0;
					if (Utils.isNotEmpty(numberOfPeerstoLaunchStr) && Utils.isNotEmpty(numberThreadsStr)) {
						numberOfPeerstoLaunch = Integer.parseInt(numberOfPeerstoLaunchStr);
						numberThreads = Integer.parseInt(numberThreadsStr);
						for (int peerCtr = 1; peerCtr <= numberOfPeerstoLaunch; peerCtr++) {
							int nodePort = nodeStartPort + peerCtr - 1;
							Node n = null;
							try {
								n = new Node(nodePort, numberThreads, myTrackingServer, dirToWatch, peerCtr);
								n.start();
								idNodeMap.put(peerCtr, n);
								nodeidFilesMap.put(peerCtr, n.getFilesFromTheWatchedDirectory(0, true));
							} catch (IOException e) {
								LoggingUtils.logError(logger, e, "Failed starting the peer-port =%s", nodePort);
							}

						}
						for (int ctr = 1; ctr <= numberOfPeerstoLaunch; ctr++) {
							idNodeMap.get(ctr).join();
						}

					}
				} else if (op.name.equals(DOWNLOAD_FILE_COMMAND)) {
					/**
					 * We will have the machineIds to start random downloads
					 */
					String machineIdStr = op.params.get("machineId");
					String filesToDownloadStr = op.params.get("filesToDownload");
					int machineId = Integer.parseInt(machineIdStr);
					int filesToDownload = Integer.parseInt(filesToDownloadStr);
					List<String> files = getAllFilesOtherThan(machineId);
					if (files.size() > 0) {
						for (int j = 0; j < filesToDownload; j++) {
							int fileIdx = randomGenerator.nextInt(files.size());
							Node n = idNodeMap.get(machineId);
							String fileName = files.get(fileIdx);
							
							try {
								n.findAndDownloadFile(fileName);
							} catch (IOException e) {
								LoggingUtils.logError(logger, e, "Error downloading the file=%s", fileName);
							} catch (Exception e){
								LoggingUtils.logError(logger, e, "Error Trying to downloading the file=%s, fileIdx=%s,files=%s, machineId=%s; idNodeMap=%s", fileName,
										fileIdx, files, machineId,idNodeMap);
							}
						}

					}
				} else if (op.name.equals(DOWNLOAD_FILE_RANDOM_COMMAND)) {
					/**
					 * We will start random downloads on all the known peers We
					 * can even invoke the local file for this operation
					 * ofcourse the peer has to take the decision
					 */
					String individualDownloadsToBeginStr = op.params.get("individualDownloadsToBegin");
					int individualDownloadsToBegin = 0;
					List<String> files = getAllFilesOtherThan(-1);
					if (Utils.isNotEmpty(individualDownloadsToBeginStr)) {
						individualDownloadsToBegin = Integer.parseInt(individualDownloadsToBeginStr);
						if (files.size() > 0 && idNodeMap.size() > 0) {
							for (Entry<Integer, Node> entry : idNodeMap.entrySet()) {
								for (int j = 0; j < individualDownloadsToBegin; j++) {
									int fileIdx = randomGenerator.nextInt(files.size());
									String fileName = files.get(fileIdx);
									try {
										entry.getValue().findAndDownloadFile(fileName);
									} catch (IOException e) {
										LoggingUtils.logError(logger, e, "Error downloading the file=%s", fileName);
									}
								}
							}

						}
					}

				}
			}
			/**
			 * After all the downloads are over we can shutdown
			 */
			if (idNodeMap.size() > 0) {
				boolean unfinishedDownloadPresent = true;
				while (unfinishedDownloadPresent) {

					for (int ij = 1; ij <= numberOfPeerstoLaunch; ij++) {
						Node n = idNodeMap.get(ij);
						if (n != null) {
							if (n.hasUnfinishedDownloads()) {
								unfinishedDownloadPresent = true;
								break;
							} else {
								unfinishedDownloadPresent = false;
							}
						}
					}
					// if reached here and
					if (unfinishedDownloadPresent) {
						continue;
					} else {
						for (int ij = 1; ij <= numberOfPeerstoLaunch; ij++) {
							Node n = idNodeMap.get(ij);
							if (n != null) {
								n.shutdown();
							}
						}
					}
					
					Thread.sleep(SLEEP_TIME_PEER_SHUTDOWN);
				}

			}
		}

		LoggingUtils.logInfo(logger, "Testing rounds are over for TestSuite =%s,\n here is the summary \n %s",
				this.testSuite, this.roundSummaries);

	}

	/**
	 * Use a set internally to remove duplicates
	 * 
	 * @param machineId
	 * @return
	 */
	private List<String> getAllFilesOtherThan(int machineId) {
		Set<String> uniqueFileNames = new HashSet<String>();
		for (Entry<Integer, List<String>> entry : nodeidFilesMap.entrySet()) {
			if (machineId != entry.getKey()) {
				for (String fileName : entry.getValue()) {
					uniqueFileNames.add(fileName);
				}
			}
		}
		List<String> retList = new ArrayList<String>(uniqueFileNames);
		return retList;

	}

	private static TestSuite getParsedTestSuite(String filePath) {
		BufferedReader in = null;
		StringBuilder sb = new StringBuilder();
		try {
			in = new BufferedReader(new FileReader(filePath));
			String str = null;
			while ((str = in.readLine()) != null) {
				sb.append(str);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {

				}
			}
		}
		XStream xstream = new XStream();
		xstream.alias("testsuite", TestSuite.class);
		xstream.alias("round", Round.class);
		xstream.alias("operation", Operation.class);

		TestSuite suite = (TestSuite) xstream.fromXML(sb.toString());

		return suite;
	}

}
