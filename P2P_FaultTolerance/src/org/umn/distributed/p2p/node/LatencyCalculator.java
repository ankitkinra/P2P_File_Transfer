package org.umn.distributed.p2p.node;

import java.util.Arrays;

import org.umn.distributed.p2p.common.Machine;

public class LatencyCalculator {

	private static final int IP_PART1_JUMP = 100;
	private static final int IP_PART2_JUMP = 10;
	private static final int IP_PART3_JUMP = 5;
	private static final int IP_PART4_JUMP = 2;
	private static final int IP_PORT_JUMP = 1;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Machine m1 = new Machine("101.2.5.6", 5000);
		Machine m2 = new Machine("105.4.8.6", 5003);
		Machine m3 = new Machine("102.3.6.6", 5000);
		System.out.println(calculateLatency(m1, m3));

	}

	public static int calculateLatency(Machine m1, Machine m2) {
		/**
		 * <pre>
		 * IP Address + PORT creates the latency
		 * Discussed with chatrola
		 * IPAddress == A1.B1.C1.D1:p1 ;; A2.B2.C2.D2:p2
		 * Then latency == (A1-A2)*T1 +(B1-B2)*T2 + (C1-C2)*T3 + (D1-D2)*T4 + (P1-P2)*T5
		 * </pre>
		 */
		IPSeparator ip1 = new IPSeparator(m1.getIP());
		IPSeparator ip2 = new IPSeparator(m2.getIP());

		return latencyCalc1(ip1, m1.getPort(), ip2, m2.getPort());
	}

	private static int latencyCalc1(IPSeparator ip1, int port1, IPSeparator ip2, int port2) {
		int latency = (ip1.part1 - ip2.part1) * IP_PART1_JUMP;
		latency += (ip1.part2 - ip2.part2) * IP_PART2_JUMP;
		latency += (ip1.part3 - ip2.part3) * IP_PART3_JUMP;
		latency += (ip1.part4 - ip2.part4) * IP_PART4_JUMP;
		latency += (port1 - port2) * IP_PORT_JUMP;
		return Math.abs(latency);
	}

	private static class IPSeparator {
		int part1;
		int part2;
		int part3;
		int part4;

		public IPSeparator(String ipAddress) {
			String[] ipArr = ipAddress != null ? ipAddress.split("\\.") : null;
			if (ipArr != null && ipArr.length == 4) {
				part1 = Integer.parseInt(ipArr[0]);
				part2 = Integer.parseInt(ipArr[1]);
				part3 = Integer.parseInt(ipArr[2]);
				part4 = Integer.parseInt(ipArr[3]);
			} else {
				throw new IllegalArgumentException("ipAddress is not correct format, ipAddress=" + ipAddress);
			}
		}
	}

}
