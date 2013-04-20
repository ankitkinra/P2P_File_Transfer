package org.umn.distributed.p2p.common;

import java.io.IOException;

import org.apache.log4j.Logger;

public abstract class BasicServer implements TcpServerDelegate {
	protected Logger logger = Logger.getLogger(this.getClass());
	private TCPServer tcpServer;
	protected int port;
	protected Machine myInfo;

	public BasicServer(int port, int numTreads) {
		this.port = port;
		this.tcpServer = new TCPServer(this, numTreads);
	}

	public void start() throws Exception {
		logger.info("****************Starting Tracking Server****************");
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

	public void stop() {
		this.tcpServer.stop();
		stopSpecific();
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

	protected abstract byte[] handleSpecificRequest(String message);
	
	protected abstract void stopSpecific();

	protected abstract void startSpecific();


}
