import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TestClient {

	private static HashMap<Integer, String> networkMap = new HashMap<Integer, String>();
	
	private static int portAddress = 20000;
	private static String localAddress = NetworkUtility.getLocalAddress();
	
	private final static int TEST_COUNT = 10000;
	
	public static void main(String[] args) {
		BufferedReader input = null;
		FileInputStream fileStream = null;
		
		try {
			// Load network information from network.config file
			Properties configuration = new Properties();
			fileStream = new FileInputStream("network.config");
			configuration.load(fileStream);
			fileStream.close();
			
			
			// Reading nodes IP addresses from the configuration file
			String peerList = configuration.getProperty("NODES");
			
			if (peerList != null) {
				String[] peers = peerList.split(",");
				
				for (int i = 0; i < peers.length; i++) {
					if (IPAddressValidator.validate(peers[i].trim())) {
						networkMap.put(i + 1, peers[i].trim());
					} else {
						System.out.println("One of the Peer's IP address in the configuration file is invalid. Exiting...");
						System.exit(0);
					}					
				}
				
				if (networkMap.isEmpty()) {
					System.out.println("No nodes(peers) present in Configuration File. No nodes... No Distributed Hash Table... Bye...");
					System.exit(0);
				}
			}
						
			input = new BufferedReader(new InputStreamReader(System.in));
			String peerAddress, fileName;
			
			// Display different choices to the user
			System.out.println("\nWhat do you want to test?");
			System.out.println("1.Register");
			System.out.println("2.Search");
			System.out.println("3.Download");
			System.out.println("4.Throughput Test");
			System.out.println("5.Exit.");
			System.out.print("Enter choice and press ENTER:");
			int option = 0;

			// Check if the user has entered only numbers.
			try {
				option = Integer.parseInt(input.readLine());
			} catch (NumberFormatException e) {
				System.out.println("Wrong choice. Try again!!!");
				System.exit(0);
			}

			switch (option) {
				case 1:
					System.out.println("\nGenerating " + TEST_COUNT + " random filenames for registration.");
					testRegister();
					break;
					
				case 2:
					System.out.println("\nSearching " + TEST_COUNT + " random filenames for registration.");
					testSearch();
					break;
	
				case 3:
					System.out.println("\nEnter file name you want to download:");
					fileName = input.readLine();
					input.readLine();
					testDownload(fileName);
					break;
					
				case 4:
					String id = getKey(localAddress);
					
					fileName = "p".concat(id).concat("file1kb.test");
					put(fileName, localAddress);
					fileName = "p".concat(id).concat("file50kb.test");
					put(fileName, localAddress);
					fileName = "p".concat(id).concat("file500kb.test");
					put(fileName, localAddress);
					fileName = "p".concat(id).concat("file1mb.test");
					put(fileName, localAddress);
					fileName = "p".concat(id).concat("file50mb.test");
					put(fileName, localAddress);
					fileName = "p".concat(id).concat("file100mb.test");
					put(fileName, localAddress);
					fileName = "p".concat(id).concat("file400mb.test");
					put(fileName, localAddress);
					
					System.out.println("\nEnter size of the file you want to download (Example: 1kb) :");
					fileName = input.readLine();
					fileName = "p".concat(getKey(localAddress)).concat("file").concat(fileName).concat(".test");
					input.readLine();
					testThroughput(fileName);
					break;
					
				case 5:
					System.out.println("Thanks for using this system.");
					System.exit(0);
					break;
				default:
					System.out.println("Wrong choice. Try again!!!");
					break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void testSearch() {
		long startTime, endTime;
        double time, avgTime;
		int errorCount = 0, successCount = 0;
		int clientId = Integer.parseInt(getKey(localAddress));
		
		long startKey = clientId * TEST_COUNT;
		long endKey = (clientId * TEST_COUNT) +  TEST_COUNT - 1;
		
		System.out.println("startKey = " + startKey);
		System.out.println("endKey = " + endKey);
		
		startTime = System.currentTimeMillis();
		
		for (long i = startKey; i <= endKey; i++) {
			if (get(Long.toString(i)) != null) {
				successCount++;
			} else {
				errorCount++;
			}
			
			if ((successCount + errorCount) % 1000 == 0) {
				System.out.print((successCount + errorCount)/ 1000 + "k ");
			}
		}
		
		endTime = System.currentTimeMillis();
		time = (endTime - startTime) / 1000.0;
		avgTime = time / (double) TEST_COUNT;
		
		System.out.println("\nSearch Operation at Client " + clientId + " with Total number Clients " + networkMap.size());
		System.out.println("TOTAL TESTS : " + TEST_COUNT + " SUCCESS : " + successCount + " ERROR : " + errorCount);
		System.out.println("TOTAL TIME : " + time + " seconds");
		System.out.println("AVERAGE TIME : " + avgTime + " seconds");
	}
	
	private static void testThroughput(String fileName) {
		long startTime, endTime, totalTime, fileSize;
		double time, avgSpeed;
		System.out.println("Test Started...");
		String peerAddress = get(fileName);
		
		startTime = System.currentTimeMillis();
		FileUtility.downloadFile(peerAddress, portAddress, fileName, false);
		endTime = System.currentTimeMillis();
		totalTime = (endTime - startTime);
		File file = new File("downloads/" + fileName);
		fileSize = file.length();
		//file.delete();
		
		//time = (double) Math.round(totalTime / 1000.0);
		avgSpeed = (fileSize / (double) totalTime) * 1000;
		
		System.out.println("TOTAL TIME: " + totalTime + " milliseconds");
		System.out.println("FILE SIZE: " + fileSize + " bytes");
		System.out.println("Average speed for downloading " + fileName + " is " + avgSpeed + " Bytes per seconds.");
	}
	
	private static void testRegister() {
		long startTime, endTime;
        double time, avgTime;
		int errorCount = 0, successCount = 0;
		int clientId = Integer.parseInt(getKey(localAddress));
		
		long startKey = clientId * TEST_COUNT;
		long endKey = (clientId * TEST_COUNT) +  TEST_COUNT - 1;
		
		startTime = System.currentTimeMillis();
		
		System.out.println("startKey = " + startKey);
		System.out.println("endKey = " + endKey);
		
		for (long i = startKey; i <= endKey; i++) {
			//if (put(Long.toString(i), Long.toString(i))) {
			if (put(Long.toString(i), Long.toString(i))) {
				successCount++;
			} else {
				errorCount++;
			}
			
			if ((successCount + errorCount) % 1000 == 0) {
				System.out.print((successCount + errorCount)/ 1000 + "k ");
			}
		}
		
		endTime = System.currentTimeMillis();
		time = (endTime - startTime) / 1000.0;
		avgTime = time / (double) TEST_COUNT;
		
		System.out.println("\nRegister Operation at Client " + clientId + " with Total number Clients " + networkMap.size());
		System.out.println("TOTAL TESTS : " + TEST_COUNT + " SUCCESS : " + successCount + " ERROR : " + errorCount);
		System.out.println("TOTAL TIME : " + time + " seconds");
		System.out.println("AVERAGE TIME : " + avgTime + " seconds");
	}
	
	private static void testDownload(String fileName) {
		long startTime, endTime, totalTime = 0, totalFileSize = 0;
		double time, avgSpeed;
		System.out.println("Test Started...");
		String peerAddress = get(fileName);
		
		for (int i = 0; i < TEST_COUNT; i++) {
			startTime = System.currentTimeMillis();
			FileUtility.downloadFile(peerAddress, portAddress, fileName, false);
			endTime = System.currentTimeMillis();
			totalTime += (endTime - startTime);
			File file = new File("downloads/" + fileName);
			totalFileSize += file.length();
			file.delete();
		}
		
		time = (double) Math.round(totalTime / 1000.0);
		avgSpeed = (totalFileSize / (1024 * 1024)) / time;
		
		System.out.println("TOTAL TIME : " + time + " seconds");
		System.out.println("Average speed for downloading " + TEST_COUNT + " files is " + avgSpeed + " MBps.");
	}
	
	/***
	 * This method performs a hash on the key and returns the node id where the key is located.
	 * We get the hashCode of the input KEY string which is a number and then we perform MOD operation on that hashCode.
	 * The hashCode for a given string will always be same for that string.
	 * @param key	Key on which hash to be performed i.e. whose node has to be found.
	 * @return		Returns a node id (integer) where the key is located.
	 */
	private static int hash(String key) {
		// Total number of servers = Total number of entries in networkMap
		int totalServers = networkMap.size();
		
		// Get hash value of string using JAVA's hashCode() method
		long hashCode = key.hashCode();
		
		// hashCode may be negative. So, make it positive in case it's negative
		hashCode = (hashCode < 0) ? -hashCode : hashCode;
		
		// Add 1 because MOD of hashCode and totalServers may be 0
		int hash = (int) (hashCode % totalServers) + 1;
		return hash;
	}
	
	private static String getKey(String value) {
		System.out.println(networkMap);
		for (Map.Entry node : networkMap.entrySet()) {
			if (node.getValue().toString().equalsIgnoreCase(value)) {
				return node.getKey().toString();
			}
		}
		return null;
	}
	
	/***
	 * This methods searches for a KEY in the Distributed Hash Table (DHT) and retrieves its value if the specified KEY exist.
	 * @param key	KEY which is to be searched in the DHT.
	 * @return		Returns VALUE for the KEY specified if the KEY exist in the DHT else returns NULL.
	 */
	private static String get(String key) {
		Socket socket = null;
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		Request peerRequest = null;
		Response serverResponse	= null;
		String value = null;
		String nodeAddress = null;
		
		try {
			int node = hash(key);
			nodeAddress = networkMap.get(node);
			
			// Checking if the (KEY, VALUE) pair is on our own peer/node
			if (nodeAddress.equals(localAddress)) {
				//System.out.println("\nIf Search CLIENT " + DistributedHashTable.getHashTable() + "\n" + DistributedHashTable.getReplicatedHashTable() + "\n");
				value = FileTransferSystem.getFromHashTable(key);
			} else {
				//System.out.println("\nElse Search CLIENT " + DistributedHashTable.getHashTable() + "\n" + DistributedHashTable.getReplicatedHashTable() + "\n");
				
				// Make connection with server using the specified Host Address and Port portAddress
		        socket = new Socket(nodeAddress, portAddress);
		        
		        // Initializing output stream using the socket's output stream
		        out = new ObjectOutputStream(socket.getOutputStream());
		        out.flush();
		        
		        // Initializing input stream using the socket's input stream
		        in = new ObjectInputStream(socket.getInputStream());

				// Setup a Request object with Request Type = PUT and Request Data = KEY,VALUE
				peerRequest = new Request();
				peerRequest.setRequestType("GET");
				peerRequest.setRequestData(key);
				out.writeObject(peerRequest);
				
		        // Read the response message from the server
		        serverResponse = (Response) in.readObject();
		        
		        if (serverResponse.getResponseCode() == 200) {
					value = serverResponse.getResponseData().toString().split(",")[1].trim();
				}
			}
		} catch(Exception e) {
			//e.printStackTrace();
		} finally {
			try {
				// Closing all streams. Close the stream only if it is initialized 
				if (out != null)
					out.close();
				
				if (in != null)
					in.close();
				
				if (socket != null)
					socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return value;
	}
	
	/***
	 * This methods adds a (KEY,VALUE) pair in the Distributed Hash Table (DHT).
	 * @param key	KEY should be 24 bytes (12 characters) maximum.
	 * @param value	VALUE should be 1000 bytes (500 characters) maximum.
	 * @return	Returns true if key is added in the DHT successfully else returns false.
	 */
	private static boolean put(String key, String value) {
		Socket socket = null;
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		Request peerRequest = null;
		Response serverResponse	= null;
		String data = key + "," + value;
	        
		try {
			//startTime = System.currentTimeMillis();
			
			int node = hash(key);
			String nodeAddress = networkMap.get(node);
			
			if (nodeAddress.equals(localAddress)) {
				//System.out.println("\nIf Search CLIENT " + DistributedHashTable.getHashTable() + "\n" + DistributedHashTable.getReplicatedHashTable() + "\n");
				boolean result = FileTransferSystem.getHashTable().containsKey(key);
				
				if (result) {
					System.out.print("\nA file with the same name is already registered by this peer. Would you like to overwrite it? (Y/N): ");
					String confirm = (new BufferedReader(new InputStreamReader(System.in))).readLine();
					
					if (confirm.equalsIgnoreCase("Y")) {
						FileTransferSystem.putInHashTable(key, value, true);
						return true;
					}
				} else {
					FileTransferSystem.putInHashTable(key, value, true);
					return true;
				}
			} else {
				//System.out.println(String.format("\nADDING (%s,%s) at %d:%s", key, value, node, nodeAddress));
				
				// Make connection with server using the specified Host Address and Port portAddress
		        socket = new Socket(nodeAddress, portAddress);
		        
		        // Initializing output stream using the socket's output stream
		        out = new ObjectOutputStream(socket.getOutputStream());
		        out.flush();
		        
		        // Initializing input stream using the socket's input stream
		        in = new ObjectInputStream(socket.getInputStream());
	
				// Setup a Request object with Request Type = PUT and Request Data = KEY,VALUE
				peerRequest = new Request();
				peerRequest.setRequestType("PUT");
				peerRequest.setRequestData(data);
				out.writeObject(peerRequest);
				
		        // Read the response message from the server
		        serverResponse = (Response) in.readObject();
		        
		        if (serverResponse.getResponseCode() == 200) {					
					return true;
				} else {
					System.out.println(serverResponse.getResponseData());
					System.exit(0);
					return false;
				}
			} 
		} catch(Exception e) {
			//e.printStackTrace();
		} finally {
			try {
				// Closing all streams. Close the stream only if it is initialized 
				if (out != null)
					out.close();
				
				if (in != null)
					in.close();
				
				if (socket != null)
					socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return false;
	}
}
