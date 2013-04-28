package org.umn.distributed.p2p.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.security.MessageDigest;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;
import org.umn.distributed.p2p.node.NodeProps;
import org.umn.distributed.p2p.server.ServerProps;

/**
 * IPAddress verification idea from
 * 
 * <pre>
 * http://www.java2s.com/Code/Java/Network-Protocol/DetermineifthegivenstringisavalidIPv4orIPv6address.htm
 * </pre>
 * 
 * @author akinra
 * 
 */
public class Utils {
	private static Logger logger = Logger.getLogger(Utils.class);
	private static Pattern VALID_IPV4_PATTERN = null;
	private static Pattern VALID_IPV6_PATTERN = null;
	private static final String IPV4_PATTERN = "(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])";
	private static final String IPV6_PATTERN = "([0-9a-f]{1,4}:){7}([0-9a-f]){1,4}";

	static {
		try {
			VALID_IPV4_PATTERN = Pattern.compile(IPV4_PATTERN, Pattern.CASE_INSENSITIVE);
			VALID_IPV6_PATTERN = Pattern.compile(IPV6_PATTERN, Pattern.CASE_INSENSITIVE);
		} catch (PatternSyntaxException e) {
			logger.error("Unable to compile pattern", e);
		}
	}

	public static boolean isIPV4Address(String ipAddress) {
		Matcher m1 = VALID_IPV4_PATTERN.matcher(ipAddress);
		return m1.matches();
	}

	public static boolean isIPV6Address(String ipAddress) {
		Matcher m2 = VALID_IPV6_PATTERN.matcher(ipAddress);
		return m2.matches();
	}

	private static String myIP = null;

	public static boolean isEmpty(String str) {
		return str == null || str.trim().length() == 0;
	}
	
	public static boolean isNotEmpty(String str) {
		return !isEmpty(str);
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
			Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
			while (en.hasMoreElements()) {
				NetworkInterface intf = en.nextElement();
				Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses();
				while (enumIpAddr.hasMoreElements()) {
					InetAddress inetAddress = enumIpAddr.nextElement();
					if (!inetAddress.isLoopbackAddress() && !inetAddress.isLinkLocalAddress()) {
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
		String encoding = ServerProps.ENCODING;
		if (ServerProps.ENCODING == null) {
			encoding = NodeProps.ENCODING;
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
		String encoding = ServerProps.ENCODING;
		if (ServerProps.ENCODING == null) {
			encoding = NodeProps.ENCODING;
		}
		return byteToString(data, encoding);
	}

	public static String[] getKeyAndValuefromFragment(String commandFragment) {
		return getStringSplitToArr(commandFragment, SharedConstants.COMMAND_VALUE_SEPARATOR);
	}

	public static String[] getKeyAndValuefromFragment(String commandFragment, int limit) {
		return getStringSplitToArr(commandFragment, SharedConstants.COMMAND_VALUE_SEPARATOR, limit);
	}

	public static String[] splitCommandIntoFragments(String command) {
		return getStringSplitToArr(command, SharedConstants.COMMAND_PARAM_SEPARATOR_REGEX, 0);
	}

	public static String[] splitCommandIntoFragments(String command, int limit) {
		return getStringSplitToArr(command, SharedConstants.COMMAND_PARAM_SEPARATOR_REGEX, limit);
	}

	public static String[] getStringSplitToArr(String commandFragment, String keyValueSeparator) {

		return commandFragment.split(keyValueSeparator, 0);

	}

	public static String[] getStringSplitToArr(String commandFragment, String keyValueSeparator, int limit) {
		return commandFragment.split(keyValueSeparator, limit);

	}

	public static byte[] createChecksum(String filename) throws Exception {
		InputStream fis = new FileInputStream(filename);

		byte[] buffer = new byte[1024];
		MessageDigest complete = MessageDigest.getInstance(SharedConstants.MESSAGE_DIGEST_NAME);
		int numRead;

		do {
			numRead = fis.read(buffer);
			if (numRead > 0) {
				complete.update(buffer, 0, numRead);
			}
		} while (numRead != -1);

		fis.close();
		return complete.digest();
	}
}
