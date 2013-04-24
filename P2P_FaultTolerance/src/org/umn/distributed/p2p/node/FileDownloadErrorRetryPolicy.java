package org.umn.distributed.p2p.node;

import org.apache.log4j.Logger;
import org.umn.distributed.p2p.common.LoggingUtils;
import org.umn.distributed.p2p.node.Constants.DOWNLOAD_ERRORS;

public class FileDownloadErrorRetryPolicy implements DownloadRetryPolicy {
	Logger log = Logger.getLogger(getClass());

	@Override
	public boolean shouldRetry(DownloadQueueObject taskToRetry) {
		Integer fileDwnldErrors = taskToRetry.getDownloadErrorMap().get(DOWNLOAD_ERRORS.FILE_CORRUPT);
		LoggingUtils.logInfo(log, "taskToRetry = %s; fileDwnldErrors=%s", taskToRetry, fileDwnldErrors);
		if (fileDwnldErrors != null && fileDwnldErrors < NodeProps.MAX_ATTEMPTS_TO_DOWNLOAD_COURRUPT_FILE) {
			return true;
		} else if(allPeersFailed(fileDwnldErrors)){
			return true;
		}
		return false;
	}

	private boolean allPeersFailed(Integer fileDwnldErrors) {
		// TODO Auto-generated method stub
		return false;
	}

}
