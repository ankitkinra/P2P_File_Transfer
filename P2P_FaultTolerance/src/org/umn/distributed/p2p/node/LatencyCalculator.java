package org.umn.distributed.p2p.node;

import org.umn.distributed.p2p.common.Machine;
import org.umn.distributed.p2p.common.Utils;

public class LatencyCalculator {
	private static final int IP_PART1_JUMP = 113;
	private static final int IP_PART2_JUMP = 109;
	private static final int IP_PART3_JUMP = 107;
	private static final int IP_PART4_JUMP = 109;
	private static final int IP_PORT_JUMP = 101;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Machine m1 = new Machine("101.2.5.6", 5000);
		Machine m2 = new Machine("105.4.8.6", 5003);
		Machine m3 = new Machine("0:0:0:0:0:ffff:6502:506", 5000);
		Machine m4 = new Machine("0:0:0:0:0:ffff:6902:506", 5000);
		System.out.println(calculateLatency(m4, m3));
		System.out.println(calculateLatency(m1, m2));
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
		IPSeparator ip1 = getIPSeparated(m1);

		IPSeparator ip2 = getIPSeparated(m2);

		return ip1.latencyCalc1(ip2);
	}

	private static IPSeparator getIPSeparated(Machine m1) {
		IPSeparator ip1 = null;
		if (Utils.isIPV4Address(m1.getIP())) {
			ip1 = new IPSeparatorIPv4(m1.getIP(), m1.getPort());
		} else if (Utils.isIPV6Address(m1.getIP())) {
			ip1 = new IPSeparatorIPv6(m1.getIP(), m1.getPort());
		}
		return ip1;
	}

	private static abstract class IPSeparator {
		protected int part1;
		protected int part2;
		protected int part3;
		protected int part4;
		protected int port;

		public int latencyCalc1(IPSeparator ip2) {
			int latency = 0;
			latency = (this.part1 - ip2.part1) * IP_PART1_JUMP;
			latency += (this.part2 - ip2.part2) * IP_PART2_JUMP;
			latency += (this.part3 - ip2.part3) * IP_PART3_JUMP;
			latency += (this.part4 - ip2.part4) * IP_PART4_JUMP;
			latency += (this.port - ip2.port) * IP_PORT_JUMP;
			latency = Math.abs(latency);
			latency = (latency % 50) + 1; // latency between 100 - 5000
			latency = latency * 100;
			return latency;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("IPSeparator [part1=");
			builder.append(part1);
			builder.append(", part2=");
			builder.append(part2);
			builder.append(", part3=");
			builder.append(part3);
			builder.append(", part4=");
			builder.append(part4);
			builder.append(", port=");
			builder.append(port);
			builder.append("]");
			return builder.toString();
		}

	}

	private static class IPSeparatorIPv4 extends IPSeparator {

		public IPSeparatorIPv4(String ipAddress, int port) {
			String[] ipArr = ipAddress != null ? ipAddress.split("\\.") : null;
			if (ipArr != null && ipArr.length == 4) {
				part1 = Integer.parseInt(ipArr[0]);
				part2 = Integer.parseInt(ipArr[1]);
				part3 = Integer.parseInt(ipArr[2]);
				part4 = Integer.parseInt(ipArr[3]);
				this.port = port;
			} else {
				throw new IllegalArgumentException("ipAddress is not correct format, ipAddress=" + ipAddress);
			}
		}

	}

	private static class IPSeparatorIPv6 extends IPSeparator {

		public IPSeparatorIPv6(String ipAddress, int port) {
			String[] ipArr = ipAddress != null ? ipAddress.split(":") : null;
			if (ipArr != null && ipArr.length == 8) {
				// adding two 16 bit numbers and then assigning them to part
				part1 = Integer.parseInt(ipArr[4], 16);
				part2 = Integer.parseInt(ipArr[5], 16);
				part3 = Integer.parseInt(ipArr[6], 16);
				part4 = Integer.parseInt(ipArr[7], 16);
				this.port = port;
			} else {
				throw new IllegalArgumentException("ipAddress is not correct format, ipAddress=" + ipAddress);
			}
		}

	}

}
