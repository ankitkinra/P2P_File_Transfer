package org.umn.distributed.p2p.node;

public interface DownloadRetryPolicy {

	boolean shouldRetry(DownloadQueueObject taskToRetry);

}
