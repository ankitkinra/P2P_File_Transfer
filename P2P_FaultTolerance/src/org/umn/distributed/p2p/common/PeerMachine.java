package org.umn.distributed.p2p.common;

import java.util.Comparator;

import org.umn.distributed.p2p.node.NodeProps;

/**
 * In addition to machine, this peer machine at every peer will contain the
 * latency information, which is the (current) load and time delay from the
 * latency matrix
 * 
 * @author akinra
 * 
 */
public class PeerMachine extends Machine {
	private long latencyMillis;
	/**
	 * when passing the peerMachine to the download queue. Create a new
	 * peerMachine and add currentLoad to it No setter is provided so that this
	 * is enforced
	 */
	private int currentLoad;
	private double avgTimeToServiceRequest;

	public PeerMachine(String iP, int port, long latencyMillis, int currentLoad) {
		super(iP, port);
		this.currentLoad = currentLoad;
		this.latencyMillis = latencyMillis;
	}

	public PeerMachine(String iP, int port, long latencyMillis, int currentLoad, double avgTimeToService) {
		super(iP, port);
		this.currentLoad = currentLoad;
		this.latencyMillis = latencyMillis;
		this.avgTimeToServiceRequest = avgTimeToService;
	}

	public long getLatencyMillis() {
		return latencyMillis;
	}

	public int getCurrentLoad() {
		return currentLoad;
	}

	public double getAvgTimeToServiceRequest() {
		return avgTimeToServiceRequest;
	}

	public void setAvgTimeToServiceRequest(double avgTimeToServiceRequest) {
		this.avgTimeToServiceRequest = avgTimeToServiceRequest;
	}

	@Override
	public String toString() {
		return "PeerMachine [latencyMillis=" + latencyMillis + ", currentLoad="
				+ currentLoad + ", avgTimeToServiceRequest="
				+ avgTimeToServiceRequest + ", latencyWeight=" + latencyWeight
				+ ", currentLoadWeight=" + currentLoadWeight + "]";
	}

	/**
	 * Default policy latencyWeight is 10 times less important than
	 * currentLoadWeight
	 */
	double latencyWeight = NodeProps.peerSelectionLatencyWeight; // 0.01
	double currentLoadWeight = NodeProps.peerSelectionLoadWeight; // 0.1

	public static final Comparator<PeerMachine> PEER_SELECTION_POLICY = new Comparator<PeerMachine>() {
		/**
		 * lesser the weight, better the peer
		 */
		@Override
		public int compare(PeerMachine o1, PeerMachine o2) {
			double peer1Weight = getPeerWeight(o1);
			double peer2Weight = getPeerWeight(o2);
			return Double.compare(peer1Weight, peer2Weight);
		}

		private double getPeerWeight(PeerMachine machine) {
			return machine.getCurrentLoad() * machine.currentLoadWeight + machine.getLatencyMillis()
					* machine.latencyMillis;
		}

		/**
		 * <code>
		 * machine.getCurrentLoad() * machine.getAvgTimeToServiceRequest() == probabilistic time when server is free
		 * </code>
		 * 
		 * @param machine
		 * @return
		 */
		private double getPeerWeight2(PeerMachine machine) {
			return machine.getCurrentLoad() * machine.getAvgTimeToServiceRequest() * machine.currentLoadWeight
					+ machine.getLatencyMillis() * machine.latencyMillis;
		}
	};

}
