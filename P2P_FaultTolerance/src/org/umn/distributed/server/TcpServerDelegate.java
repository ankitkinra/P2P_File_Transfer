package org.umn.distributed.server;

public interface TcpServerDelegate {
	public byte[] handleRequest(byte[] request);
}
