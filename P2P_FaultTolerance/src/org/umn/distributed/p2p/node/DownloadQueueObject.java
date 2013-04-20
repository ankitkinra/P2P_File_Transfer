package org.umn.distributed.p2p.node;

import java.util.EnumMap;
import java.util.List;

import org.umn.distributed.p2p.common.Machine;
import org.umn.distributed.p2p.node.Constants.DOWNLOAD_ACTIVITY;
import org.umn.distributed.p2p.node.Constants.DOWNLOAD_ERRORS;

public class DownloadQueueObject {
	private String fileToDownload;
	private List<Machine> peersToDownloadFrom;
	private DOWNLOAD_ACTIVITY downloadActivityStatus = null;
	private EnumMap<DOWNLOAD_ERRORS, Integer> downloadErrorMap = new EnumMap<Constants.DOWNLOAD_ERRORS, Integer>(
			DOWNLOAD_ERRORS.class);
}
