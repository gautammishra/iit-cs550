import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/***
 * This class provides various methods to create and manage logs.
 * Example: Peer download logs and Server logs
 */
public class LogUtility {

	private String logFile = "";
	private BufferedWriter writer = null;
	private final String logLocation = "logs/";

	/***
	 * Constructor which initializes the log file
	 * @param logType	Type of log to be worked on. Peer/Server log
	 */
	public LogUtility(String logType) {
		try {
			if (logType.equalsIgnoreCase("Peer")) {
				logFile = "peer.server.log";
			} else if (logType.equalsIgnoreCase("Server")) {
				logFile = "server.log";
			} else if (logType.equalsIgnoreCase("Replication")) {
				logFile = "replication.log";
			}
			// Create a logs folder if it doesn't exist
			File file = new File(logLocation);
			if (!file.exists())
				file.mkdir();
			
			writer = new BufferedWriter(new FileWriter(logLocation + logFile, true));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/***
	 * This method is used to write a text to the log file.
	 * @param logText	Text to be appended to the log file.
	 * @return			Returns true if write is successful else returns false
	 */	
	public boolean write(String logText) {
		boolean isWriteSuccess = false;
		try {
			String timeLog = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss").format(Calendar.getInstance().getTime());
			if (writer != null) {
				logText = String.format("%s => %s", timeLog, logText);
				writer.write(logText);
				String newline = System.getProperty("line.separator");
				writer.write(newline);
				isWriteSuccess = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return isWriteSuccess;
	}
	
	/***
	 * This method prints the content of the log file.
	 */
	public void print() {
		BufferedReader br = null;
		File file = new File(logFile);
		int charCount = 0;
		
		System.out.println("\nLOG");
		System.out.println("=========================================================================");
		
		try {
			br = new BufferedReader(new FileReader(logLocation + logFile));
			String line = null;
			while ((line = br.readLine()) != null) {
				System.out.println(line);
				charCount += line.length();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (charCount == 0) {
			System.out.println("NO LOGS TO PRINT");
		}

		System.out.println("=========================================================================");
	}
	
	/***
	 * This method closes the file stream so that the log file can be accessed by other methods.
	 */
	public void close() {
		try {
			if (writer != null) {
				String newline = System.getProperty("line.separator");
				writer.write(newline);
				writer.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/***
	 * This method is called whenever the garbage collector is called. Acts as destructor.
	 */
	@Override
	protected void finalize() throws Throwable {
		if (writer != null) {
			writer.close();
		}
		super.finalize();
	}
	
	/*public static void main(String[] args) {
		LogUtility log = new LogUtility("peer");
		log.write("Hello");
		log.close();
	}*/
}