package org.umn.distributed.p2p.node;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.umn.distributed.p2p.common.LoggingUtils;
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
	private Logger log = Logger.getLogger(getClass());
	private ExecutorService service;
	private Queue<DownloadQueueObject> failedTaskQ = new LinkedList<DownloadQueueObject>();
	private String outputFolder = null;
	private Machine myMachineInfo = null;
	private DownloadRetryPolicy downloadRetryPolicy = null;
	private FailureQueueMonitor failedQMonitor = null;
	private AtomicInteger activeDownloadCount;
	private Object updateThreadMonitorObj;

	/*
	 * TODO need to keep a thread looking at the failedTask as it could fail due
	 * to server failure or corrupt file
	 */
	public DownloadService(AtomicInteger activeDownloadCount, DownloadRetryPolicy downloadRetryPolicy,
			Comparator<DownloadQueueObject> downloadPriorityAssignment, int initDownloadQueueCapacity,
			String outputFolder, Machine myMachine, Object updateThreadMonitorObj) {
		this.service = Executors.newFixedThreadPool(initDownloadQueueCapacity);
		this.outputFolder = outputFolder;
		this.myMachineInfo = myMachine;
		this.downloadRetryPolicy = downloadRetryPolicy;
		this.activeDownloadCount = activeDownloadCount;
		this.failedQMonitor = new FailureQueueMonitor();
		this.updateThreadMonitorObj = updateThreadMonitorObj;
	}

	public void start() {
		//this.failedQMonitor.start();
	}

	public void stop() {
		/*try {
			this.failedQMonitor.isCancelled = true;
			this.failedQMonitor.interrupt();
		} catch (Exception e) {
			
		} finally {
			
		}*/
		this.service.shutdown();

	}

	/**
	 * Capture the request
	 * 
	 * @param dwnldStatus
	 */
	public void acceptDownloadRequest(DownloadStatus dwnldStatus) {
		activeDownloadCount.incrementAndGet();
		this.service.execute(new DownloadQueueObject(dwnldStatus, this.myMachineInfo, this.failedTaskQ,
				this.outputFolder, this.activeDownloadCount, this.updateThreadMonitorObj));
	}

	/**
	 * Aim is to monitor the failedProcessQueue and then see what task to
	 * process based on the retry policy this thread will activate every
	 * FAILED_TASK_RETRY_INTERVAL and will pick MAX_TASK_TO_RETRY_EVERY_INTERVAL
	 * to give the new task a better chance to run
	 * 
	 * @author akinra
	 * 
	 */
	private class FailureQueueMonitor extends Thread {
		boolean isCancelled = false;

		public void run() {
			while (!isCancelled) {
				try {
					Thread.sleep(NodeProps.FAILED_TASK_RETRY_INTERVAL);
				} catch (InterruptedException e) {
					// eat exception
				}
				int counter = 0;
				while (!failedTaskQ.isEmpty()) {
					if (counter++ > NodeProps.MAX_TASK_TO_RETRY_EVERY_INTERVAL) {
						DownloadQueueObject taskToRetry = failedTaskQ.poll();
						if (downloadRetryPolicy.shouldRetry(taskToRetry)) {
							service.execute(taskToRetry);
						} else {
							LoggingUtils.logInfo(log, "not retrying the task=%s", taskToRetry);
						}
					}
				}
			}
		}
	}

}
