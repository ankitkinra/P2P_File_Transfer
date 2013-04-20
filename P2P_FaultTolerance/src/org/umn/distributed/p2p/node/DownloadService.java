package org.umn.distributed.p2p.node;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class DownloadService {
	private PriorityBlockingQueue<DownloadQueueObject> downloadQueue = null;

	public DownloadService(AtomicInteger currentDownloads, DownloadRetryPolicy downloadRetryPolicy,
			Comparator<DownloadQueueObject> downloadPriorityAssignment, int initDownloadQueueCapacity) {
		downloadQueue = new PriorityBlockingQueue<DownloadQueueObject>(initDownloadQueueCapacity,
				downloadPriorityAssignment);
	}

	public void acceptDownloadRequest(DownloadStatus dwnldStatus) {

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
