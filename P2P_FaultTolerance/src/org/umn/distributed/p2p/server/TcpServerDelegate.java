package org.umn.distributed.p2p.server;

public interface TcpServerDelegate {
	public byte[] handleRequest(byte[] request);
}
