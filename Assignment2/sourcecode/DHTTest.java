import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Properties;

public class DHTTest {
	
	private static HashMap<Integer, String> networkMap = new HashMap<>();
	private static int portNumber;
	
	public static void main(String[] args) throws IOException {
		//networkMap = DistributedHashTable.getNetworkMap();
		portNumber = DistributedHashTable.getPeerServerPort();
		
		// Load network information from network.config file
		Properties configuration = new Properties();
		FileInputStream fileStream = new FileInputStream("network.config");
		configuration.load(fileStream);
		fileStream.close();
		
		// Reading nodes IP addresses from the configuration file
		String peerList = configuration.getProperty("NODES");
		if (peerList == null) {
			System.out.println("Configuration could not be loaded. Test cannot be performed.");
			System.exit(0);
		}
		
		String[] peers = peerList.split(",");
		
		for (int i = 0; i < peers.length; i++) {
			networkMap.put(i + 1, peers[i].trim());
		}

		System.out.println(networkMap);	      
		
		if (!networkMap.isEmpty()) {
			BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
	        
	        System.out.print("Enter the operation you want to test? (PUT, GET, DEL): ");
	        String operation = input.readLine();
	        System.out.print("Enter the number of clients you want to test on? (1-8): ");
	        int numClients = Integer.parseInt(input.readLine());
	        System.out.print("Enter the number of operations you want to perform? (Example, 100000): ");
	        long opCount = Long.parseLong(input.readLine());
	        
	        if ((operation.trim().equalsIgnoreCase("PUT") || operation.trim().equalsIgnoreCase("GET") || operation.trim().equalsIgnoreCase("DEL"))
	        	&& numClients >= 1 && numClients <= 8 && opCount >= 0) {
				
				System.out.println(String.format("******* %s OPERATION TEST STARTED *******", operation.trim().toUpperCase()));
				for (int i = 1; i <= numClients; i++) {
					new TestClient(i, operation.trim().toUpperCase(), opCount).start();	
				}
			} else {
				System.out.println("\nOne of the input was wrong. Please run the program again. Bye...");
			}
		} else {
			System.out.println("Configuration could not be loaded. Test cannot be performed.");
		}
		
		
	}
	
	private static class TestClient extends Thread {
		private String testFunction = null;
		private int clientId = 0;
		private long testCount = 0;
		
		public TestClient(int clientId, String testFunction, long testCount) {
			this.testFunction = testFunction;
			this.testCount = testCount;
			this.clientId = clientId;
		}
		
		// Thread implementation for Peer to serve as CLient
		public void run() {
			
	        long startTime, endTime;
	        double time, avgTime;
			int errorCount = 0, successCount = 0;
			
			long startKey = clientId * testCount;
			long endKey = (clientId * testCount) +  testCount - 1;
			
			startTime = System.currentTimeMillis();
			System.out.println("startKey = " + startKey);
			System.out.println("endKey = " + endKey);
			
			if (testFunction.equalsIgnoreCase("PUT")) {			
				for (long i = startKey; i <= endKey; i++) {
					if (put(Long.toString(i), Long.toString(i))) {
						successCount++;
					} else {
						errorCount++;
					}
					
					/*if ((successCount + errorCount) % 1000 == 0) {
						System.out.print((successCount + errorCount)/ 1000 + "k ");
					}*/
				}
				
				endTime = System.currentTimeMillis();
				time = (endTime - startTime) / 1000.0;
				avgTime = time / (double) testCount;
				
				System.out.println("\nPUT Operation at Client " + clientId);
				System.out.println("TOTAL TESTS : " + testCount + " SUCCESS : " + successCount + " ERROR : " + errorCount);
				System.out.println("TOTAL TIME : " + time + " seconds");
				System.out.println("AVERAGE TIME : " + avgTime + " seconds");
				
			} else if (testFunction.equalsIgnoreCase("GET")) {
				for (long i = startKey; i <= endKey; i++) {
					if (get(Long.toString(i)) != null) {
						successCount++;
					} else {
						errorCount++;
					}
					
					/*if ((successCount + errorCount) % 1000 == 0) {
						System.out.print((successCount + errorCount)/ 1000 + "k ");
					}*/
				}
				
				endTime = System.currentTimeMillis();
				time = (endTime - startTime) / 1000.0;
				avgTime = time / (double) testCount;
				
				System.out.println("\nGET Operation at Client " + clientId);
				System.out.println("TOTAL TESTS : " + testCount + " SUCCESS : " + successCount + " ERROR : " + errorCount);
				System.out.println("TOTAL TIME : " + time + " seconds");
				System.out.println("AVERAGE TIME : " + avgTime + " seconds");
				
				
			} else if (testFunction.equalsIgnoreCase("DEL")) {
				for (long i = startKey; i <= endKey; i++) {
					if (delete(Long.toString(i))) {
						successCount++;
					} else {
						errorCount++;
					}
					
					/*if ((successCount + errorCount) % 1000 == 0) {
						System.out.print((successCount + errorCount)/ 1000 + "k ");
					}*/
				}
				
				endTime = System.currentTimeMillis();
				time = (endTime - startTime) / 1000.0;
				avgTime = time / (double) testCount;
				
				System.out.println("\nDELETE Operation at Client " + clientId);
				System.out.println("TOTAL TESTS : " + testCount + " SUCCESS : " + successCount + " ERROR : " + errorCount);
				System.out.println("TOTAL TIME : " + time + " seconds");
				System.out.println("AVERAGE TIME : " + avgTime + " seconds");
				
			}
		}
		
		/***
		 * This methods adds a (KEY,VALUE) pair in the Distributed Hash Table (DHT).
		 * @param key	KEY should be 24 bytes (12 characters) maximum.
		 * @param value	VALUE should be 1000 bytes (500 characters) maximum.
		 * @return	Returns true if key is added in the DHT successfully else returns false.
		 */
		private boolean put(String key, String value) {
			Socket socket = null;
			ObjectInputStream in = null;
			ObjectOutputStream out = null;
			Request peerRequest = null;
			Response serverResponse	= null;
			String data = key + "," + value;
			
			try {
				int node = hash(key);
				String nodeAddress = networkMap.get(node);
				
				//System.out.println(String.format("\nADDING (%s,%s) at %d:%s", key, value, node, nodeAddress));
				
				// Make connection with server using the specified Host Address and Port 10000
		        socket = new Socket(nodeAddress, portNumber);
		        
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
					return false;
				}
		        
			} catch(Exception e) {
				e.printStackTrace();
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
		
		/***
		 * This methods searches for a KEY in the Distributed Hash Table (DHT) and retrieves its value if the specified KEY exist.
		 * @param key	KEY which is to be searched in the DHT.
		 * @return		Returns VALUE for the KEY specified if the KEY exist in the DHT else returns NULL.
		 */
		private String get(String key) {
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
				
				// Make connection with server using the specified Host Address and Port 10000
		        socket = new Socket(nodeAddress, portNumber);
		        
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
					value = serverResponse.getResponseData().split(",")[1].trim();
				}
			} catch(Exception e) {
				e.printStackTrace();
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
		 * This methods deletes a (KEY,VALUE) pair from the Distributed Hash Table (DHT) using KEY. It does nothing if the KEY doesn't exist in the DHT.
		 * @param key	KEY of the (KEY,VALUE) pair which has to be deleted from the DHT.
		 * @return		Returns true if the (KEY,VALUE) pair is successfully deleted from the DHT else returns false.
		 */
		private boolean delete(String key) {
			Socket socket = null;
			ObjectInputStream in = null;
			ObjectOutputStream out = null;
			Request peerRequest = null;
			Response serverResponse	= null;
			
			try {
				int node = hash(key);
				String nodeAddress = networkMap.get(node);
				
				// Make connection with server using the specified Host Address and Port 10000
		        socket = new Socket(nodeAddress, portNumber);
		        
		        // Initializing output stream using the socket's output stream
		        out = new ObjectOutputStream(socket.getOutputStream());
		        out.flush();
		        
		        // Initializing input stream using the socket's input stream
		        in = new ObjectInputStream(socket.getInputStream());

				// Setup a Request object with Request Type = PUT and Request Data = KEY,VALUE
				peerRequest = new Request();
				peerRequest.setRequestType("DELETE");
				peerRequest.setRequestData(key);
				out.writeObject(peerRequest);
				
		        // Read the response message from the server
		        serverResponse = (Response) in.readObject();
		        //System.out.print((String) serverResponse.getResponseData());
		        
		        if (serverResponse.getResponseCode() == 200) {
					return true;
				} else {
					return false;
				}
		        
			} catch(Exception e) {
				e.printStackTrace();
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
		
		/***
		 * This method performs a hash on the key and returns the node id where the key is located.
		 * We get the hashCode of the input KEY string which is a number and then we perform MOD operation on that hashCode.
		 * The hashCode for a given string will always be same for that string.
		 * @param key	Key on which hash to be performed i.e. whose node has to be found.
		 * @return		Returns a node id (integer) where the key is located.
		 */
		private int hash(String key) {
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
	}
}
