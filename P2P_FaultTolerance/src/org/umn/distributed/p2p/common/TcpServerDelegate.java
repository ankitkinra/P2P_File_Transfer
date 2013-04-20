package org.umn.distributed.p2p.common;

public interface TcpServerDelegate {
	public byte[] handleRequest(byte[] request);

	/**
	 * This method will be invoked when the TCPServer is given a task to perform
	 * that is listen to a request and during that listening some error has
	 * occurred. This is the way to let the original object handle the error
	 * 
	 * @param e
	 */
	public void handleServerException(Exception e);
}
