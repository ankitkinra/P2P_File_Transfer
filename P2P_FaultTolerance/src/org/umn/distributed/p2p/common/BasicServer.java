package org.umn.distributed.p2p.common;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;

import org.apache.log4j.Logger;

public abstract class BasicServer extends Thread implements TcpServerDelegate {
	protected Logger logger = Logger.getLogger(this.getClass());
	private TCPServer tcpServer;
	protected int port;
	protected Machine myInfo;

	public BasicServer(int port, int numTreads, HashSet<String> commandsWhoHandleTheirOutput) {
		this.port = port;
		this.tcpServer = new TCPServer(this, numTreads, commandsWhoHandleTheirOutput);
		myInfo = new Machine(Utils.getLocalServerIp(), this.port);
	}

	public BasicServer(int port, int numTreads) {
		this(port, numTreads, null);
	}

	public void run() {
		logger.info("****************Starting Tracking Server****************");
		try {
			this.port = this.tcpServer.startListening(this.port);
			LoggingUtils.logInfo(logger, "Started server, myInfo=%s", myInfo);
			intializeSpecific();
		} catch (IOException ioe) {
			logger.error("Error starting tcp server. Stopping now", ioe);
			this.shutdown();
			throw new RuntimeException(ioe);
		}
	}

	public void shutdown() {
		this.tcpServer.stop();
		shutdownSpecific();
	}

	@Override
	public byte[] handleRequest(byte[] request) {
		try {
			String req = Utils.byteToString(request);
			return handleSpecificRequest(req);
		} catch (Exception e) {
			logger.error("Exception handling request in Tracking Server", e);
			return Utils.stringToByte(SharedConstants.COMMAND_FAILED + SharedConstants.COMMAND_PARAM_SEPARATOR
					+ e.getMessage());
		}

	}

	@Override
	public void handleRequest(byte[] request, OutputStream socketOutput) {
		try {
			String req = Utils.byteToString(request);
			handleSpecificRequest(req, socketOutput);
		} catch (Exception e) {
			logger.error("Exception handling request in Tracking Server", e);
		}

	}

	protected void handleSpecificRequest(String req, OutputStream socketOutput) {
		// Empty declaration
	}

	protected abstract byte[] handleSpecificRequest(String message);

	protected abstract void shutdownSpecific();

	protected abstract void intializeSpecific();

}
