package org.umn.distributed.p2p.testfrmwrk;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.umn.distributed.p2p.common.ClientProps;
import org.umn.distributed.p2p.common.LoggingUtils;
import org.umn.distributed.p2p.common.Machine;
import org.umn.distributed.p2p.common.Utils;

import com.thoughtworks.xstream.XStream;

public class TestClient {
	private static final String READ_ITEM_COMMAND_NAME = "readItem";
	private static final String READ_ITEMS_COMMAND_NAME = "readItems";
	private static final String POST_OPERATION_NAME = "post";
	private String xmlTestCasePath = null;
	private Logger logger = Logger.getLogger(TestClient.class);
	private TestSuite testSuite = null;
	private Machine myCoord;
	private HashMap<Integer, RoundSummary> roundSummaries = new HashMap<Integer, RoundSummary>();
	private Random randomGenerator = new Random();

	public TestClient(String xmlFilePath, String coordinatorIp,
			int coordinatorPort) {
		this.xmlTestCasePath = xmlFilePath;
		this.testSuite = getParsedTestSuite(xmlFilePath);
		this.myCoord = new Machine(coordinatorIp, coordinatorPort);

	}

	public static void showUsage() {
		System.out.println("Usage:");
		System.out
				.println("Run tests: ./runtests.sh <Test Config File> <Coordinator Ip> <Coordinator Port> <config file path>");
	}

	public static void main(String[] args) {
		if (args.length == 4) {
			String xmlTestFile = args[0];
			String coopIP = args[1];
			try {
				int coopPort = Integer.parseInt(args[2]);
				if (!Utils.isValidPort(coopPort)) {
					System.out.println("Invalid port");
					showUsage();
					return;
				}
				ClientProps.loadProperties(args[3]);
				TestClient tc = new TestClient(xmlTestFile, coopIP, coopPort);
				System.out.println(tc.testSuite);
				tc.startTest();
			} catch (NumberFormatException nfe) {
				System.out.println("Invalid port");
				showUsage();
			}
		} else {
			showUsage();
		}
	}


	private void startTest() {
		for (int i = 0; i < this.testSuite.rounds.size(); i++) {
			// init new summary
			Round r = this.testSuite.rounds.get(i);
			RoundSummary rs = new RoundSummary(r);
			this.roundSummaries.put(i, rs);
			logger.info("In the testClient starting round =" + r.name);
			for (Operation op : r.operations) {
				/**
				 * Before each operation we need to start a timer and get the
				 * servers where we need to send the operation to
				 */
				if (op.name.equals(POST_OPERATION_NAME)) {
					String rootArticlesToPostStr = op.params.get("root");
					String childArticlesToPostStr = op.params.get("child");
					int rootArticlesToPost = 0, repliesToPost = 0;
					if (!Utils.isEmpty(rootArticlesToPostStr)) {
						rootArticlesToPost = Integer
								.parseInt(rootArticlesToPostStr);
					}
					if (!Utils.isEmpty(childArticlesToPostStr)) {
						repliesToPost = Integer
								.parseInt(childArticlesToPostStr);
					}
					try {
						//performActivity
					} catch (Exception e) {
						logger.error("Error in posting articles", e);
					}
				} else if (op.name.equals(READ_ITEMS_COMMAND_NAME)) {
					/**
					 * Need to get a random server and read its content and
					 * record the time After that we can give an analysis which
					 * says that do we have all the articles that we have posted
					 * in this server or not
					 */

					for (int j = 0; j < op.repeatations + 1; j++) {
						logger.info("In read items, iteration number = " + j);
						try {
							//verifyCorrectness
						} catch (Exception e) {
							logger.error("Error in readArticles articles", e);
						}
					}

				} else if (op.name.equals(READ_ITEM_COMMAND_NAME)) {

					String relativeArticleIdStr = op.params.get("id");
					int relativeArticleId = 0;
					if (!Utils.isEmpty(relativeArticleIdStr)) {
						relativeArticleId = Integer
								.parseInt(relativeArticleIdStr);
					}
					try {
						//verifyCorrectness
					} catch (Exception e) {
						logger.error("Error in reading articleId = "
								+ relativeArticleId, e);
					}
				}
			}
		}

		LoggingUtils
				.logInfo(
						logger,
						"Testing rounds are over for TestSuite =%s,\n here is the summary \n %s",
						this.testSuite, this.roundSummaries);

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
