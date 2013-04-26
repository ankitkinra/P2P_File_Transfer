package org.umn.distributed.p2p.common;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.umn.distributed.p2p.node.LatencyCalculator;

public class TCPClient {
	protected static Logger logger = Logger.getLogger(TCPClient.class);

	public static byte[] sendData(Machine remoteMachine, Machine ownMachine, byte[] data) throws IOException {
		if (logger.isDebugEnabled()) {
			logger.debug("Send " + Utils.byteToString(data) + " to " + remoteMachine);
		}

		long delay = LatencyCalculator.calculateLatency(ownMachine, remoteMachine);
		try {
			LoggingUtils.logInfo(logger, "Introducing the latency =%s between peers = %s and %s", delay, ownMachine,
					remoteMachine);
			Thread.sleep(delay);
		} catch (InterruptedException e) {
			LoggingUtils.logError(logger, e, "Error while waiting by thread = %s", Thread.currentThread().getName());
		}

		/**
		 * This will open a local socket and send the data to the remoteMachine
		 */
		Socket clientSocket = null;
		int buffSize = SharedConstants.DEFAULT_BUFFER_LENGTH;
		int count = 0;
		InputStream is = null;
		byte[] buffer = new byte[buffSize];
		byte[] outputBuffer = null;
		try {
			clientSocket = new Socket(remoteMachine.getIP(), remoteMachine.getPort());
			clientSocket.getOutputStream().write(data);
			clientSocket.getOutputStream().flush();
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			is = clientSocket.getInputStream();

			while ((count = is.read(buffer)) > -1) {
				bos.write(buffer, 0, count);
				bos.flush();
			}
			outputBuffer = bos.toByteArray();
			bos.close();
		} catch (IOException ioe) {
			logger.error("Error connecting to " + remoteMachine, ioe);
			throw ioe;
		} finally {
			try {
				if (clientSocket != null) {
					clientSocket.close();
				}
			} catch (IOException ios) {
				throw ios;
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Data received at client " + Utils.byteToString(buffer));
		}
		LoggingUtils.logDebug(logger, "Data recieved at client = %s for the command =%s which was sent to machine=%s",
				Arrays.toString(buffer), Utils.byteToString(data), remoteMachine);
		return outputBuffer;
	}

	public static byte[] sendDataGetFile(Machine remoteMachine, Machine ownMachine, byte[] data,
			String fileWriteLocation) throws IOException {
		if (logger.isDebugEnabled()) {
			logger.debug("Send " + Utils.byteToString(data) + " to " + remoteMachine);
		}

		long delay = LatencyCalculator.calculateLatency(ownMachine, remoteMachine);
		try {
			LoggingUtils.logInfo(logger, "Introducing the latency =%s between peers = %s and %s", delay, ownMachine,
					remoteMachine);
			Thread.sleep(delay);
		} catch (InterruptedException e) {
			LoggingUtils.logError(logger, e, "Error while waiting by thread = %s", Thread.currentThread().getName());
		}

		/**
		 * This will open a local socket and send the data to the remoteMachine
		 */
		Socket clientSocket = null;
		int buffSize = SharedConstants.DEFAULT_BUFFER_LENGTH;
		int count = 0;
		InputStream is = null;
		byte[] buffer = new byte[buffSize];
		clientSocket = new Socket(remoteMachine.getIP(), remoteMachine.getPort());
		clientSocket.getOutputStream().write(data);
		clientSocket.getOutputStream().flush();
		FileOutputStream fos = null;
		int bufferCounter = 0;
		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance(SharedConstants.MESSAGE_DIGEST_NAME);
			is = clientSocket.getInputStream();
			fos = new FileOutputStream(fileWriteLocation);
			count = 0;
			/*
			 * TODO If the first buffer is zero length then does that mean that
			 * the file was not present
			 */
			while ((count = is.read(buffer)) >= 0) {
				if (bufferCounter == 0) {
					if (Arrays.equals(buffer, SharedConstants.FILE_NOT_FOUND)) {
						throw new FileNotFoundException(String.format("The file =%s was not found on the peer =%s",
								fileWriteLocation, remoteMachine));
					}
				}
				fos.write(buffer, 0, count);
				// calculate the digest here as well
				digest.update(buffer, 0, count);
				bufferCounter++;

			}

		} catch (IOException ioe) {
			logger.error("Error connecting to " + remoteMachine, ioe);
			throw ioe;
		} catch (NoSuchAlgorithmException e) {
			logger.error("Error calculating the checksum:", e);

		} finally {
			fos.close();
			is.close();
			clientSocket.close();
		}
		return digest != null ? digest.digest() : null;

	}
}
