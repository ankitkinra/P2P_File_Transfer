package org.umn.distributed.p2p.node;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.umn.distributed.p2p.common.LoggingUtils;
import org.umn.distributed.p2p.common.Machine;
import org.umn.distributed.p2p.common.SharedConstants;
import org.umn.distributed.p2p.common.Utils;

public class UploadService {
	private ExecutorService service;
	private Logger logger = Logger.getLogger(getClass());
	private AtomicInteger activeUploadCount;

	public void start() {
	}

	public void stop() {
		this.service.shutdown();

	}

	public UploadService(AtomicInteger currentUploads, int initUploadQueueCapacity) {
		this.service = Executors.newFixedThreadPool(initUploadQueueCapacity);
		this.activeUploadCount = currentUploads;
	}

	public void uploadFile(String fileNameFullyQualified, Machine machine, OutputStream socketOutput) {
		this.service.execute(new UploadQueueObject(fileNameFullyQualified, machine, socketOutput));
	}

	private class UploadQueueObject implements Runnable {

		private String fileName = null;
		private Machine machineToSend = null;
		private OutputStream socketOutput = null;
		byte[] buffer = new byte[NodeProps.FILE_BUFFER_LENGTH];

		public UploadQueueObject(String fileNameFullyQualified, Machine machine, OutputStream socketOutput) {
			this.fileName = fileNameFullyQualified;
			this.machineToSend = machine;
			this.socketOutput = socketOutput;

		}

		public void run() {
			/*
			 * Find the file and if it exists start the transfer
			 */
			LoggingUtils.logInfo(logger, "Starting upload of file = %s to peer =%s", this.fileName, this.machineToSend);
			activeUploadCount.incrementAndGet();
			File fileToSend = null;
			try {
				fileToSend = new File(this.fileName);
				if (fileToSend.isFile() && fileToSend.canRead()) {
					try {
						FileInputStream fileInputStream = new FileInputStream(fileToSend);
						int number = 0;
						LoggingUtils.logInfo(logger, "file = %s to peer =%s;fileToSend = %s", this.fileName,
								this.machineToSend, fileToSend.length());
						int count = 0;
						while ((number = fileInputStream.read(buffer)) != -1) {
							LoggingUtils.logInfo(logger, "Writing buffer number=%s for file = %s to peer =%s", count,this.fileName,
									this.machineToSend);
							this.socketOutput.write(buffer, 0, number);
							this.socketOutput.flush();
							count++;
						}
					} catch (FileNotFoundException e) {
						LoggingUtils.logError(logger, e, "File=%s not found and sending error to the peer=%s",
								this.fileName, this.machineToSend);
						try {
							this.socketOutput.write(Utils.stringToByte(SharedConstants.COMMAND_FAILED));
						} catch (IOException e1) {
							LoggingUtils.logError(logger, e1,
									"File=%s not found and also could not send error to the peer=%s", this.fileName,
									this.machineToSend);
						}
					} catch (IOException e) {
						LoggingUtils.logError(logger, e, "IOException while tranferring file File=%s ", this.fileName);
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				activeUploadCount.decrementAndGet();
			}
		}

	}

}
