import java.net.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NetworkUtility {

	public static void main(String args[]) throws SocketException {
		System.out.println(getLocalAddress());
	}
	
	/***
	 * This method retrieves the IP address assigned to the system.
	 * @return	Returns the IP address assigned to the system.
	 */
	public static String getLocalAddress() {
		String localAddress = null;
		String loopbackAddress = InetAddress.getLoopbackAddress().getHostAddress();
		boolean found = false;
		
		try {
			Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
			for (NetworkInterface netint : Collections.list(nets)) {
				Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
				for (InetAddress inetAddress : Collections.list(inetAddresses)) {
					/*out.printf("InetAddress: %s\n", inetAddress);
					out.printf("Loopback InetAddress: %s\n", inetAddress.getLoopbackAddress().getHostAddress());*/
					if (inetAddress instanceof Inet4Address && !inetAddress.getHostAddress().equals(loopbackAddress)) {
						localAddress = inetAddress.getHostAddress();
						found = true;
						break;
					}
				}
				if (found) break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return localAddress;
	}
	
	private static Pattern pattern;
	private static Matcher matcher;

	private static final String IPADDRESS_PATTERN = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
			+ "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
			+ "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";

	/**
	 * Validate ip address with regular expression
	 * @param ip	ip address for validation
	 * @return 		Returns true if IP address is valid else returns false
	 */
	public static boolean validate(final String ip) {
		pattern = Pattern.compile(IPADDRESS_PATTERN);
		matcher = pattern.matcher(ip);
		return matcher.matches();
	}

}
