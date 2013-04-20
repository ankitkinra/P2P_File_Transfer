package org.umn.distributed.p2p.common;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

public class Machine {
	protected Logger logger = Logger.getLogger(this.getClass());

	public static final String FORMAT_START = "[";
	public static final String FORMAT_END = "]";

	private String IP;
	private int port;
	private int extPort;

	public Machine(String iP, int port, int extPort) {
		this.IP = iP;
		this.port = port;
		this.extPort = extPort;
	}

	public Machine(String iP, int port) {
		this(iP, port, 0);
	}

	public Machine(String iP, String port) {
		this(iP, Integer.parseInt(port), 0);
	}

	public String getIP() {
		return IP;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getExternalPort() {
		return extPort;
	}

	public void setExternalPort(int extPort) {
		this.extPort = extPort;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((IP == null) ? 0 : IP.hashCode());
		result = prime * result + port;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Machine other = (Machine) obj;
		if (IP == null) {
			if (other.IP != null)
				return false;
		} else if (!IP.equals(other.IP))
			return false;
		if (port != other.port)
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(FORMAT_START).append(IP).append(SharedConstants.COMMAND_LIST_SEPARATOR).append(port)
				.append(SharedConstants.COMMAND_LIST_SEPARATOR).append(extPort).append(FORMAT_END);
		return builder.toString();
	}

	public static Machine parse(String machineStr) throws IllegalArgumentException {
		if (!machineStr.startsWith(FORMAT_START) || !machineStr.endsWith(FORMAT_END)) {
			throw new IllegalArgumentException("Invalid machine format=" + machineStr);
		}
		machineStr = machineStr.substring(1, machineStr.length() - 1);
		String machineParams[] = machineStr.split(SharedConstants.COMMAND_LIST_SEPARATOR);
		if (machineParams.length != 3) {
			throw new IllegalArgumentException("Invalid machine parameter number");
		}

		int internalPort = 0;
		int externalPort = 0;
		try {
			internalPort = Integer.parseInt(machineParams[1]);
			externalPort = Integer.parseInt(machineParams[2]);
			return new Machine(machineParams[0], internalPort, externalPort);
		} catch (NumberFormatException nfe) {
			throw new IllegalArgumentException("Invalid article id/parentId");
		}
	}

	public static List<Machine> parseList(String req) {
		List<Machine> listMachines = new LinkedList<Machine>();
		int index = -1;
		int start = 0;
		while ((index = req.indexOf("]", start)) > -1) {
			Machine machine = Machine.parse(req.substring(start, index + 1));
			listMachines.add(machine);
			start = index + 1;

		}
		return listMachines;
	}

	public static String convertCollectionToString(Collection<Machine> machineCollection) {
		StringBuilder sb = new StringBuilder();

		for (Machine m : machineCollection) {
			sb.append(m.toString());
		}
		return sb.toString();
	}

}
