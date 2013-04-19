package org.umn.distributed.p2p.common;

public interface TcpServerDelegate {
	public byte[] handleRequest(byte[] request);
}
