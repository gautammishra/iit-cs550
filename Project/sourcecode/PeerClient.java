import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PeerClient extends Thread {
		
	private HashMap<Integer, String> networkMap = null;
	private ArrayList<String> replicationNodes = null;
	
	private int portAddress = 0;
	private String localAddress = null;
	
	// Initialize all the local data from the global data
	public PeerClient() {
		networkMap = DistributedHashTable.getNetworkMap();
		replicationNodes = DistributedHashTable.getReplicationNodes();
		portAddress = DistributedHashTable.getPeerServerPort();
		localAddress = DistributedHashTable.getLocalAddress();
	}
	
	// Thread implementation for Peer to serve as CLient
	public void run() {
		BufferedReader input = null;
		
		try {
			input = new BufferedReader(new InputStreamReader(System.in));
			
			HashMap<String, String> hm = retrieveHashTable();
			if (hm != null) {
				for (Map.Entry e : hm.entrySet()) {
					DistributedHashTable.putInHashTable(e.getKey().toString(), e.getValue().toString(), true);
				}
			}
			
			if (replicationNodes.contains(localAddress)) {
				ConcurrentHashMap<String, HashMap<String, String>> rHashTable = retrieveReplicationHashTable();
				if (rHashTable != null) {
					DistributedHashTable.setReplicatedHashTable(rHashTable);
				}
			}
			
			//System.out.println(DistributedHashTable.getHashTable());
			//System.out.println(DistributedHashTable.getReplicatedHashTable());
			
	        long startTime, endTime;
	        double time;
			
	        String key, value, confirm;
	        
	        while (true) {
	        	// Display different choices to the user
	        	System.out.println("\nWhat do you want to do?");
		        System.out.println("1.Add a (key,value) pair.");
		        System.out.println("2.Search for a key.");
		        System.out.println("3.Delete a (key,value) pair.");
		        System.out.println("4.Exit.");
		        System.out.print("Enter choice and press ENTER:");
		        int option;
		        
		        // Check if the user has entered only numbers.
		        try {
		        	option = Integer.parseInt(input.readLine());
				} catch (NumberFormatException e) {
					System.out.println("Wrong choice. Try again!!!");
					continue;
				}
		        
		        // Handling all the choices
		        switch (option) {
		        // Adding a (KEY, VALUE) pair in the network - functionality
				case 1:
					System.out.print("\nEnter KEY (Max. 12 characters): ");
					key = input.readLine();
					// Validating key
					if (!validateKey(key))
						continue;
					
					System.out.print("Enter VALUE (Max. 500 characters): ");
					value = input.readLine();
					// Validating value
					if (!validateValue(value))
						continue;
					
					/*startTime = System.currentTimeMillis();*/
					
					if (put(key, value)) {
						System.out.println(String.format("(%s, %s) added successfully to the Distributed Hash Table.", key, value));
					} else {
						System.out.println("There was an error in adding key. Please try again later.");
					}
					
					/*endTime = System.currentTimeMillis();
					time = (double) Math.round(endTime - startTime) / 1000;
					System.out.println("Time taken: " + time + " seconds");*/
					
					break;

				// Searching VALUE for a given KEY - functionality
				case 2:
					System.out.println("\nEnter the KEY which you want to search:");
					key = input.readLine();
					
					// Validating key
					if (!validateKey(key))
						continue;
					
					startTime = System.currentTimeMillis();
					
					value = get(key);
					if (value != null) {
						System.out.println(String.format("VALUE = %s for KEY = %s", value, key));
					} else {
						System.out.println("VALUE not found for the given KEY in the Distributed Hash Table.");
					}
					
					endTime = System.currentTimeMillis();
					time = (double) Math.round(endTime - startTime) / 1000;
					System.out.println("Time taken: " + time + " seconds");
					
					break;
					
				// DELETING a (KEY, VALUE) pair in the network - functionality
				case 3:
					System.out.println("\nEnter the KEY which you want to delete:");
					key = input.readLine();
					
					// Validating key
					if (!validateKey(key))
						continue;
					
					// Confirming user's delete (KEY, VALUE) pair request
					System.out.print("\nAre you sure (Y/N)?:");
					confirm = input.readLine();
					
					if (confirm.equalsIgnoreCase("Y")) {
						startTime = System.currentTimeMillis();
						
						if (delete(key)) {
							System.out.println("(KEY, VALUE) pair was deleted successfully from the Distributed Hash Table.");
						} else {
							System.out.println("There was an error in deleting key. Please try again later.");
						}
						
						endTime = System.currentTimeMillis();
						time = (double) Math.round(endTime - startTime) / 1000;
						System.out.println("Time taken: " + time + " seconds");
					}

					break;
					
				// Handling Peer exit functionality
				case 4:
					// Confirming user's exit request
					System.out.print("\nExiting will delete all (KEY, VALUE) pairs stored on this node and will no longer be accessible by other nodes in this network. Are you sure you want to exit? (Y/N)?:");
					confirm = input.readLine();
					
					if (confirm.equalsIgnoreCase("Y")) {
						System.out.println("Thanks for using this system.");
						System.exit(0);
					}
					
					break;
					
				default:
					System.out.println("Wrong choice. Try again!!!");
					break;
				}
	        }
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				// Closing all streams. Close the stream only if it is initialized 
				if (input != null)
					input.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/***
	 * This method retrieves the Hash Table from one of the replication nodes after this peer was down so that it can re-gain its hashTable from the replication nodes.
	 * @return	Returns the hashTable (if exists) of the calling peer from the replication nodes.
	 */
	private HashMap<String, String> retrieveHashTable() {
		Socket socket = null;
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		Request peerRequest = null;
		Response serverResponse	= null;
		HashMap<String, String> hm = null;
		
		for (String nodeAddress : replicationNodes) {
			//System.out.println("Retrieving Hash Table from " + nodeAddress);
			
			if (nodeAddress.equalsIgnoreCase(localAddress)) {
				continue;
			}
			
			try {
				
				// Make connection with server using the specified Host Address and Port 10000
				socket = new Socket(nodeAddress, portAddress);

				// Initializing output stream using the socket's output stream
				out = new ObjectOutputStream(socket.getOutputStream());
				out.flush();

				// Initializing input stream using the socket's input stream
				in = new ObjectInputStream(socket.getInputStream());

				// Setup a Request object with Request Type = PUT and Request Data = KEY,VALUE
				peerRequest = new Request();
				peerRequest.setRequestType("GET_HASHTABLE");
				out.writeObject(peerRequest);

				// Read the response message from the server
				serverResponse = (Response) in.readObject();
				if (serverResponse != null && serverResponse.getResponseCode() == 200) {
					hm = (HashMap<String, String>) serverResponse.getOtherData();
				} 
					
				socket.close();
				break;
			} catch (Exception ex) {
				//ex.printStackTrace();
			} finally {
				try {
					// Closing all streams. Close the stream only if it is initialized
					if (out != null)
						out.close();

					if (in != null)
						in.close();

					if (socket != null)
						socket.close();
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
		}
		//System.out.println("hm = " + hm);
		return hm;
	}
	
	/***
	 * This method retrieves the Replication Hash Table from one of the replication nodes after this peer was down so that it can re-gain its replication data from the replication nodes.
	 * @return	Returns the replicatedHashTable (if exists) of the calling peer from the replication nodes.
	 */
	private ConcurrentHashMap<String, HashMap<String, String>> retrieveReplicationHashTable() {
		Socket socket = null;
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		Request peerRequest = null;
		Response serverResponse	= null;
		ConcurrentHashMap<String, HashMap<String, String>> hm = null;
		
		for (String nodeAddress : replicationNodes) {
			//System.out.println("Retrieving Replication Hash Table from " + nodeAddress);
			
			if (nodeAddress.equalsIgnoreCase(localAddress)) {
				continue;
			}
			
			try {
				// Make connection with server using the specified Host Address and Port 10000
				socket = new Socket(nodeAddress, portAddress);

				// Initializing output stream using the socket's output stream
				out = new ObjectOutputStream(socket.getOutputStream());
				out.flush();

				// Initializing input stream using the socket's input stream
				in = new ObjectInputStream(socket.getInputStream());

				// Setup a Request object with Request Type = PUT and Request Data = KEY,VALUE
				peerRequest = new Request();
				peerRequest.setRequestType("GET_R_HASHTABLE");
				out.writeObject(peerRequest);

				// Read the response message from the server
				serverResponse = (Response) in.readObject();
				if (serverResponse != null && serverResponse.getResponseCode() == 200) {
					hm = (ConcurrentHashMap<String, HashMap<String, String>>) serverResponse.getOtherData();
				} 
					
				socket.close();
				break;
			} catch (Exception ex) {
				//ex.printStackTrace();
			} finally {
				try {
					// Closing all streams. Close the stream only if it is initialized
					if (out != null)
						out.close();

					if (in != null)
						in.close();

					if (socket != null)
						socket.close();
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
		}
		//System.out.println("replica hm = " + hm);
		return hm;
	}
	
	/***
	 * This methods adds a (KEY,VALUE) pair in the Distributed Hash Table (DHT).
	 * @param key	KEY should be 24 bytes (12 characters) maximum.
	 * @param value	VALUE should be 1000 bytes (500 characters) maximum.
	 * @return	Returns true if key is added in the DHT successfully else returns false.
	 */
	public boolean put(String key, String value) {
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
	 * This methods forces to add a (KEY,VALUE) pair in the Distributed Hash Table (DHT). The KEY will be replaced if it exists.
	 *  The (KEY,VALUE) pair is inserted to the hashTable even if the KEY already exists.
	 * @param key	KEY should be 24 bytes (12 characters) maximum.
	 * @param value	VALUE should be 1000 bytes (500 characters) maximum.
	 * @return	Returns true if key is added in the DHT successfully else returns false.
	 */
	private boolean forcePut(String key, String value) {
		Socket socket = null;
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		Request peerRequest = null;
		Response serverResponse	= null;
		String data = key + "," + value;
		
		long startTime, endTime;
	    double time;
	        
		try {
			startTime = System.currentTimeMillis();
			
			int node = hash(key);
			String nodeAddress = networkMap.get(node);
			
			//System.out.println(String.format("\nADDING (%s,%s) at %d:%s", key, value, node, nodeAddress));
			
			// Make connection with server using the specified Host Address and Port 10000
	        socket = new Socket(nodeAddress, portAddress);
	        
	        // Initializing output stream using the socket's output stream
	        out = new ObjectOutputStream(socket.getOutputStream());
	        out.flush();
	        
	        // Initializing input stream using the socket's input stream
	        in = new ObjectInputStream(socket.getInputStream());

			// Setup a Request object with Request Type = PUT and Request Data = KEY,VALUE
			peerRequest = new Request();
			peerRequest.setRequestType("PUT_FORCE");
			peerRequest.setRequestData(data);
			out.writeObject(peerRequest);
			
	        // Read the response message from the server
	        serverResponse = (Response) in.readObject();
	        
	        if (serverResponse.getResponseCode() == 200) {
	        	endTime = System.currentTimeMillis();
				time = (double) Math.round(endTime - startTime) / 1000;
				System.out.println("Time taken: " + time + " seconds");
				
				return true;
			} else {
				System.out.println(serverResponse.getResponseData());
				return false;
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
	
	/***
	 * This methods searches for a KEY in the Distributed Hash Table (DHT) and retrieves its value if the specified KEY exist.
	 * @param key	KEY which is to be searched in the DHT.
	 * @return		Returns VALUE for the KEY specified if the KEY exist in the DHT else returns NULL.
	 */
	public String get(String key) {
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
				value = DistributedHashTable.getFromHashTable(key);
			} else {
				//System.out.println("\nElse Search CLIENT " + DistributedHashTable.getHashTable() + "\n" + DistributedHashTable.getReplicatedHashTable() + "\n");
				
				// Make connection with server using the specified Host Address and Port 10000
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
					value = serverResponse.getResponseData().split(",")[1].trim();
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
			value = searchReplica(key, nodeAddress);
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
	 * This method searches for a KEY on the Replication Nodes in case it is not able to connect to the original peer having that (KEY,VALUE) pair.
	 * @param key 	KEY which is to be searched in the Replication Hash Table (Replication Nodes).
	 * @return		Returns VALUE for the KEY specified if the KEY exist in the DHT else returns NULL.
	 */
	private String searchReplica(String key, String peerAddress) {
		Socket socket = null;
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		Request peerRequest = null;
		Response serverResponse	= null;
		String value = null;
		
		System.out.println("Searching Replica - " + peerAddress + ", key - " + key);
		for (String nodeAddress : replicationNodes) {
			try {
				// Checking if the (KEY, VALUE) pair is on our own peer/node
				if (nodeAddress.equals(localAddress)) {
					//System.out.println("\nIf Replica SERVER " + DistributedHashTable.getHashTable() + "\n" + DistributedHashTable.getReplicatedHashTable() + "\n");
					value = DistributedHashTable.getFromReplicaHashTable(key);
				} else {
					//System.out.println("\nElse Replica SERVER " + DistributedHashTable.getHashTable() + "\n" + DistributedHashTable.getReplicatedHashTable() + "\n");
					
					// Make connection with server using the specified Host Address and Port 10000
			        socket = new Socket(nodeAddress, portAddress);
			        
			        // Initializing output stream using the socket's output stream
			        out = new ObjectOutputStream(socket.getOutputStream());
			        out.flush();
			        
			        // Initializing input stream using the socket's input stream
			        in = new ObjectInputStream(socket.getInputStream());

					// Setup a Request object with Request Type = PUT and Request Data = KEY,VALUE
					peerRequest = new Request();
					peerRequest.setRequestType("R_GET");
					peerRequest.setRequestData(key);
					out.writeObject(peerRequest);
					
			        // Read the response message from the server
			        serverResponse = (Response) in.readObject();
			        
			        if (serverResponse.getResponseCode() == 200) {
						value = serverResponse.getResponseData().split(",")[1].trim();
					}
				}
			} catch(Exception ex) {
				//ex.printStackTrace();
			} finally {
				try {
					// Closing all streams. Close the stream only if it is initialized 
					if (out != null)
						out.close();
					
					if (in != null)
						in.close();
					
					if (socket != null)
						socket.close();
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
			
			if (value != null) {
				break;
			}
		}
		return value;
	}
	
	/***
	 * This methods deletes a (KEY,VALUE) pair from the Distributed Hash Table (DHT) using KEY. It does nothing if the KEY doesn't exist in the DHT.
	 * @param key	KEY of the (KEY,VALUE) pair which has to be deleted from the DHT.
	 * @return		Returns true if the (KEY,VALUE) pair is successfully deleted from the DHT else returns false.
	 */
	public boolean delete(String key) {
		Socket socket = null;
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		Request peerRequest = null;
		Response serverResponse	= null;
		
		try {
			int node = hash(key);
			String nodeAddress = networkMap.get(node);
			
			// Make connection with server using the specified Host Address and Port 10000
	        socket = new Socket(nodeAddress, portAddress);
	        
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
	
	/***
	 * This method checks whether the KEY is valid or not. A KEY is valid if it is less than 24 bytes (12 JAVA characters)
	 * @param key	Key which is to be validated.
	 * @return		Returns true is KEY is valid else false
	 */
	private boolean validateKey(String key) {
		// Check if KEY is greater than 0 bytes and not more than 24 bytes i.e. 12 characters in JAVA. We are not using any HEADER
		if (key.trim().length() == 0) {
			System.out.println("Invalid KEY.");
			return false;
		} else if (key.trim().length() > 10) {
			System.out.println("Invalid KEY. KEY should not be more than 24 bytes (12 characters).");
			return false;
		}
		return true;
	}
	
	/***
	 * This method checks whether the VALUE is valid or not. A VALUE is valid if it is less than 1000 bytes (500 JAVA characters)
	 * @param key	VALUE which is to be validated.
	 * @return		Returns true is VALUE is valid else false
	 */
	private boolean validateValue(String value) {
		// Check if VALUE is greater than 0 bytes and not more than 1000 bytes i.e. 500 characters in JAVA
		if (value.trim().length() == 0) {
			System.out.println("Invalid VALUE.");
			return false;
		} else if (value.trim().length() > 500) {
			System.out.println("Invalid VALUE. VALUE should not be more than 1000 bytes (500 characters).");
			return false;
		}
		return true;
	}
}