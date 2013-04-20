package org.umn.distributed.p2p.common;

import java.util.Comparator;

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

	public PeerMachine(String iP, int port, long latencyMillis, int currentLoad) {
		super(iP, port);
		this.currentLoad = currentLoad;
		this.latencyMillis = latencyMillis;
	}

	public long getLatencyMillis() {
		return latencyMillis;
	}

	public int getCurrentLoad() {
		return currentLoad;
	}

	/**
	 * Default policy latencyWeight is 10 times less important than
	 * currentLoadWeight
	 */
	double latencyWeight = 0.01;
	double currentLoadWeight = 0.1;

	final Comparator<PeerMachine> peerSelectionPolicy = new Comparator<PeerMachine>() {
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
			return machine.getCurrentLoad() * currentLoadWeight + machine.getLatencyMillis() * latencyMillis;
		}
	};

}
