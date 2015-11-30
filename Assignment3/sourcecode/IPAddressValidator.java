import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IPAddressValidator {
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
	
	public static void main(String[] args) {
		System.out.println(validate("192.168.206.128"));
	}
}
