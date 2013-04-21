package org.umn.distributed.p2p.node;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.umn.distributed.p2p.common.Machine;

/**
 * The task queue is downloadQueue, there would be a single thread which is
 * polling downloadQueue, once we Don't really this now as everything goes to
 * DownloadQueueObject, refactor
 * 
 * @author akinra
 * 
 */
public class DownloadService {
	private ExecutorService service;
	private Queue<DownloadQueueObject> failedTaskQ = new LinkedList<DownloadQueueObject>();
	private String outputFolder = null;
	private Machine myMachineInfo = null;
	private DownloadRetryPolicy downloadRetryPolicy = null;
	private FailureQueueMonitor failedQMonitor = null;

	/*
	 * TODO need to keep a thread looking at the failedTask as it could fail due
	 * to server failure or corrupt file
	 */
	public DownloadService(AtomicInteger currentDownloads, DownloadRetryPolicy downloadRetryPolicy,
			Comparator<DownloadQueueObject> downloadPriorityAssignment, int initDownloadQueueCapacity,
			String outputFolder, Machine myMachine) {
		this.service = Executors.newFixedThreadPool(initDownloadQueueCapacity);
		this.outputFolder = outputFolder;
		this.myMachineInfo = myMachine;
		this.downloadRetryPolicy = downloadRetryPolicy;
	}

	public void start() {
		this.failedQMonitor.start();
	}

	public void stop() {
		this.failedQMonitor.interrupt();
	}

	/**
	 * Capture the request
	 * 
	 * @param dwnldStatus
	 */
	public void acceptDownloadRequest(DownloadStatus dwnldStatus) {

		this.service.execute(new DownloadQueueObject(dwnldStatus.getFileToDownload(), dwnldStatus
				.getPeersToDownloadFrom().keySet(), this.myMachineInfo, this.failedTaskQ, this.outputFolder));
	}

	public void shutDown() {
		this.service.shutdown();
	}

	private class FailureQueueMonitor extends Thread {

	}
}
