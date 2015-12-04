import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Properties;

public class MyDHT {
	
	private static HashMap<Integer, String> networkMap = null;
	private static int portNumber;
	
	public static void connect(String fileName) {
		// networkMap = DistributedHashTable.getNetworkMap();
		portNumber = DistributedHashTable.getPeerServerPort();

		// Load network information from network.config file
		Properties configuration = new Properties();
		try {
			FileInputStream fileStream = new FileInputStream(fileName);
			configuration.load(fileStream);
			fileStream.close();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		// Reading nodes IP addresses from the configuration file
		String peerList = configuration.getProperty("NODES");
		if (peerList == null) {
			System.out.println("Network configuration could not be loaded. Test cannot be performed.");
		} else {
			networkMap = new HashMap<>();
			String[] peers = peerList.split(",");

			for (int i = 0; i < peers.length; i++) {
				networkMap.put(i + 1, peers[i].trim());
			}

			System.out.println(networkMap);
		}
	}
	
	public static boolean insert(String key, String value) {
		put(key, value);
		return true;
	}
	
	public static String lookup(String key) {
		return get(key);
	}
	
	public static boolean remove(String key) {
		delete(key);
		return true;
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
	private static boolean delete(String key) {
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
}
