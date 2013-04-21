package org.umn.distributed.p2p.common;

import java.io.OutputStream;

public interface TcpServerDelegate {
	public byte[] handleRequest(byte[] request);
	/**
	 * In case the client wants to have full control of the sending
	 * @param request
	 * @param socketOutput
	 */
	public void handleRequest(byte[] request, OutputStream socketOutput);

	/**
	 * This method will be invoked when the TCPServer is given a task to perform
	 * that is listen to a request and during that listening some error has
	 * occurred. This is the way to let the original object handle the error
	 * 
	 * @param e
	 */
	public void handleServerException(Exception e, String commandRecieved);
}
