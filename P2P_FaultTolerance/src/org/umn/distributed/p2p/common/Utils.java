package org.umn.distributed.p2p.common;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.Enumeration;

import org.apache.log4j.Logger;

public class Utils {
	private static Logger logger = Logger.getLogger(Utils.class);

	private static String myIP = null;

	public static boolean isEmpty(String str) {
		return str == null || str.trim().length() == 0;
	}

	public static boolean isNumber(String num) {
		try {
			Integer.parseInt(num);
		} catch (NumberFormatException ne) {
			return false;
		}
		return true;
	}

	public static int findFreePort(int startNumber) {
		while (!isPortAvailable(startNumber)) {
			if (!isValidPort(startNumber)) {
				return -1;
			}
			startNumber++;
		}
		return startNumber;
	}

	public static boolean isValidPort(int port) {
		if (port < 1 || port > 65535) {
			return false;
		}
		return true;
	}

	public static boolean isPortAvailable(int port) {
		if (isValidPort(port)) {
			ServerSocket sSocket = null;
			DatagramSocket dSocket = null;
			try {
				sSocket = new ServerSocket(port);
				sSocket.setReuseAddress(true);
				dSocket = new DatagramSocket(port);
				dSocket.setReuseAddress(true);
				return true;
			} catch (IOException e) {
			} finally {
				if (dSocket != null) {
					dSocket.close();
				}
				if (sSocket != null) {
					try {
						sSocket.close();
					} catch (IOException e) {
						// TODO: handle exception
					}
				}
			}

			return false;
		}
		return false;
	}

	public static String getLocalServerIp() {
		if (myIP != null) {
			return myIP;
		}
		try {
			Enumeration<NetworkInterface> en = NetworkInterface
					.getNetworkInterfaces();
			while (en.hasMoreElements()) {
				NetworkInterface intf = en.nextElement();
				Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses();
				while (enumIpAddr.hasMoreElements()) {
					InetAddress inetAddress = enumIpAddr.nextElement();
					if (!inetAddress.isLoopbackAddress()
							&& !inetAddress.isLinkLocalAddress()) {
						myIP = inetAddress.getHostAddress().toString();
						logger.debug("localhost ip found:" + myIP);
						return myIP;
					}
				}
			}
		} catch (SocketException e) {
			logger.error(e.getMessage(), e);
			return null;
		}
		logger.error("cannot find the localhost ip");
		return null;
	}

	public static byte[] stringToByte(String str, String encoding) {
		try {
			return str.getBytes(encoding);
		} catch (UnsupportedEncodingException e) {
			logger.error("invalid encoding type: " + encoding, e);
		}
		return null;
	}

	public static byte[] stringToByte(String str) {
		String encoding = Props.ENCODING;
		if (Props.ENCODING == null) {
			encoding = ClientProps.ENCODING;
		}
		return stringToByte(str, encoding);
	}

	public static String byteToString(byte[] data, String encoding) {
		try {
			if (data != null) {
				return new String(data, encoding);
			} else {
				return null;
			}
		} catch (UnsupportedEncodingException e) {
			logger.error("invalid encoding type: " + encoding, e);
		}
		return null;
	}

	public static String byteToString(byte[] data) {
		String encoding = Props.ENCODING;
		if (Props.ENCODING == null) {
			encoding = ClientProps.ENCODING;
		}
		return byteToString(data, encoding);
	}

	public static String[] getKeyAndValuefromFragment(String commandFragment) {
		return getStringSplitToArr(commandFragment,
				SharedConstants.COMMAND_VALUE_SEPARATOR);
	}
	
	public static String[] splitCommandIntoFragments(String command) {
		return getStringSplitToArr(command,
				"\\"+SharedConstants.COMMAND_PARAM_SEPARATOR);
	}

	public static String[] getStringSplitToArr(String commandFragment,
			String keyValueSeparator) {
		return commandFragment.split(keyValueSeparator);
	}

	

}