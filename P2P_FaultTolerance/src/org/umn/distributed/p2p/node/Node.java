package org.umn.distributed.p2p.node;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.umn.distributed.p2p.common.TcpServerDelegate;

public class Node implements TcpServerDelegate {

	private final AtomicInteger currentUploads = new AtomicInteger(0);
	private final AtomicInteger currentDownloads = new AtomicInteger(0);
	private int initDownloadQueueCapacity = 10;
	private DownloadRetryPolicy downloadRetryPolicy = null;
	/**
	 * A thread needs to monitor the unfinished status queue so that we can
	 * report to the initiator the download status
	 */
	private PriorityQueue<DownloadStatus> unfinishedDownloadStatus = null;

	private Comparator<DownloadQueueObject> downloadPriorityAssignment = null;
	private final DownloadService downloadService = new DownloadService(currentDownloads, downloadRetryPolicy,
			downloadPriorityAssignment, initDownloadQueueCapacity);

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] handleRequest(byte[] request) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void handleServerException(Exception e) {
		// TODO Auto-generated method stub

	}

}
