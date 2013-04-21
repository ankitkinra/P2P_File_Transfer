package org.umn.distributed.p2p.node;

import org.umn.distributed.p2p.node.Constants.DOWNLOAD_ERRORS;

public class FileDownloadErrorRetryPolicy implements DownloadRetryPolicy {

	@Override
	public boolean shouldRetry(DownloadQueueObject taskToRetry) {
		Integer fileDwnldErrors = taskToRetry.getDownloadErrorMap().get(DOWNLOAD_ERRORS.FILE_CORRUPT);
		if(fileDwnldErrors != null && fileDwnldErrors<NodeProps.MAX_ATTEMPTS_TO_DOWNLOAD_COURRUPT_FILE){
			return true;
		}
		return false;
	}

}
