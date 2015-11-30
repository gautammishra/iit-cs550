import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PeerClient extends Thread {
	
	private HashMap<Integer, String> networkMap = null;
	private ArrayList<String> replicationNodes = null;
	
	private int portAddress = 0;
	private String localAddress = null;
	private String filesLocation = null;
	
	// Initialize all the local data from the global data
	public PeerClient() {
		networkMap = FileTransferSystem.getNetworkMap();
		replicationNodes = FileTransferSystem.getReplicationNodes();
		portAddress = FileTransferSystem.getPeerServerPort();
		localAddress = FileTransferSystem.getLocalAddress();
		filesLocation = FileTransferSystem.getFilesLocation();
	}
	
	// Thread implementation for Peer to serve as CLient
	public void run() {
		BufferedReader input = null;
		
		try {
			input = new BufferedReader(new InputStreamReader(System.in));
			
			HashMap<String, String> hm = retrieveHashTable();
			if (hm != null) {
				for (Map.Entry e : hm.entrySet()) {
					FileTransferSystem.putInHashTable(e.getKey().toString(), e.getValue().toString(), true);
				}
			}
			
			if (replicationNodes.contains(localAddress)) {
				System.out.println("****** REPLICATION SERVICE STARTED ******");
				ReplicationService service = new ReplicationService(null, null, "REPLICATE");
				service.start();
			}
	        
	        long startTime, endTime;
	        double time;
			
	        String key, value, confirm, fileName;
	        
	        while (true) {
	        	// Display different choices to the user
	        	System.out.println("\nWhat do you want to do?");
		        System.out.println("1.Register a file with the peers.");
		        System.out.println("2.Search for a file.");
		        System.out.println("3.Un-register a file with the peers.");
		        System.out.println("4.Print log of this peer.");
		        System.out.println("5.Exit.");
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
		        // Registers a file in the network i.e. adds a (FILENAME, IP ADDRESS) pair in the network - functionality
				case 1:
					System.out.println("\nEnter name of the file with extension (Example: data.txt) you want to put in file sharing system:");
					fileName = input.readLine();
					
					// Checking if the user has entered something
					if(fileName.trim().length() == 0) {
						System.out.println("Invalid Filename.");
						continue;
					}
					
					File file = new File(filesLocation + fileName);
					
					// Check if file exists
					if (file.exists()) {
						startTime = System.currentTimeMillis();

						if(put(fileName, localAddress)) {
							System.out.println(fileName + " added to the file sharing system and is available to download for other peers.");
						} else {
							System.out.println("Unable to add " + fileName + " to the file sharing system. Please try again later.");
						}
						
						endTime = System.currentTimeMillis();
						time = (double) Math.round(endTime - startTime) / 1000;
						System.out.println("Time Taken: " + time + " seconds.");
					} else {
						System.out.println("File with the given filename does not exist in [" + filesLocation + "] path.");
					}
					break;

				// Searching for a file in the network - functionality
				case 2:
					System.out.println("\nEnter name of the file you want to look for:");
					fileName = input.readLine();
					String hostAddress;
					
					startTime = System.currentTimeMillis();
					value = get(fileName);
					
					endTime = System.currentTimeMillis();
					time = (double) Math.round(endTime - startTime) / 1000;
					if (value != null) {
						System.out.println("File Found. Lookup time: " + time + " seconds.");
						hostAddress = value;

						// If the file is a Text file then we can print or else only download file
						if (fileName.trim().endsWith(".txt")) {
							System.out.print("\nDo you want to download (D) or print this file (P)? Enter (D/P):");
							String download = input.readLine();
							
							if (download.equalsIgnoreCase("D")) {
								System.out.println("The file will be downloaded in the 'downloads' folder in the current location.");
								// Obtain the searched file from the specified Peer
								obtain(hostAddress, portAddress, fileName);
							} else if (download.equalsIgnoreCase("P")) {
								// Obtain the searched file from the specified Peer and print its contents
								obtain(hostAddress, portAddress, fileName);
								FileUtility.printFile(fileName);
							}
						} else {
							System.out.print("\nDo you want to download this file?(Y/N):");
							String download = input.readLine();
							if (download.equalsIgnoreCase("Y")) {
								// Obtain the searched file from the specified Peer
								obtain(hostAddress, portAddress, fileName);
							}	
						}
					}  else {
						System.out.println("File not found. Lookup time: " + time + " seconds.");
					}					
					break;
					
				// Unregistering a file from the network - functionality
				case 3:
					System.out.println("\nEnter the name of the file you want remove from file sharing system:");
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
							System.out.println("The file was successfully removed from the file sharing system.");
						} else {
							System.out.println("There was an error in removing the file. Please try again later.");
						}
						
						endTime = System.currentTimeMillis();
						time = (double) Math.round(endTime - startTime) / 1000;
						System.out.println("Time taken: " + time + " seconds");
					}

					break;
					
				// Printing the download log
				case 4:
					(new LogUtility("peer")).print();
					break;
					
				// Handling Peer exit functionality
				case 5:
					// Confirming user's exit request
					System.out.print("\nThe files shared by this peer will no longer be accessible by other peers in this network. Are you sure you want to exit? (Y/N)?:");
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
	 * This method is used to download the file from the requested Peer.
	 * @param hostAddress 	IP Address of the peer used to download the file
	 * @param port			Port of the per used to download the file
	 * @param fileName		Name of the file to be downloaded
	 */
	private void obtain(String hostAddress, int port, String fileName) {
		boolean isDownloaded = false;
		long startTime = System.currentTimeMillis();
		
		if (!FileUtility.downloadFile(hostAddress, port, fileName, false)) {
			List<String> backupNodes = FileTransferSystem.getReplicationNodes();
			
			//System.out.println(backupNodes);
			for (String node : backupNodes) {
				if(FileUtility.downloadFile(node, port, fileName, true)) {
					isDownloaded = true;
					break;
				}
			}
		} else {
			isDownloaded = true;
		}
		
		long endTime = System.currentTimeMillis();
		double time = (double) Math.round(endTime - startTime) / 1000;

		if (isDownloaded) {
			System.out.println("File downloaded successfully in " + time + " seconds.");
		} else {
			System.out.println("Unable to connect to the host. Unable to  download file. Try using a different peer if available.");
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
				
				// Make connection with server using the specified Host Address and Port portAddress
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
					hm = (HashMap<String, String>) serverResponse.getResponseData();
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
	 * This method is used to register a file in the network. KEY means the name of the file and VALUE means the IP address of the peer storing that file.
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
		
		long startTime, endTime;
	    double time;
	        
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
						
						ReplicationService service = new ReplicationService(key, value, "REGISTER");
						service.start();
						return true;
					}
				} else {
					FileTransferSystem.putInHashTable(key, value, true);
					
					ReplicationService service = new ReplicationService(key, value, "REGISTER");
					service.start();
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
				peerRequest.setRequestType("REGISTER");
				peerRequest.setRequestData(data);
				out.writeObject(peerRequest);
				
		        // Read the response message from the server
		        serverResponse = (Response) in.readObject();
		        
		        if (serverResponse.getResponseCode() == 200) {
		        	/*endTime = System.currentTimeMillis();
					time = (double) Math.round(endTime - startTime) / 1000;
					System.out.println("Time taken: " + time + " seconds");*/
					
					return true;
				} else if (serverResponse.getResponseCode() == 300) {
					System.out.print("\nA VALUE with the specified KEY already exists in  the Distributed Hash Table. Would you like to overwrite it? (Y/N): ");
					String confirm = (new BufferedReader(new InputStreamReader(System.in))).readLine();
					
					if (confirm.equalsIgnoreCase("Y")) {
						return forcePut(key, value);
					}
				} else {
					//System.out.println(serverResponse.getResponseData());
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
	
	/***
	 * This methods forces to register a file in the network i.e. add a (KEY,VALUE) pair in the Distributed Hash Table (DHT). 
	 * The KEY will be replaced if it exists. The (KEY,VALUE) pair is inserted to the hashTable even if the KEY already exists.
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
			
			// Make connection with server using the specified Host Address and Port portAddress
	        socket = new Socket(nodeAddress, portAddress);
	        
	        // Initializing output stream using the socket's output stream
	        out = new ObjectOutputStream(socket.getOutputStream());
	        out.flush();
	        
	        // Initializing input stream using the socket's input stream
	        in = new ObjectInputStream(socket.getInputStream());

			// Setup a Request object with Request Type = PUT and Request Data = KEY,VALUE
			peerRequest = new Request();
			peerRequest.setRequestType("REGISTER_FORCE");
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
	 * This methods searches for a file in the network i.e. searched for KEY in the Distributed Hash Table (DHT) 
	 * and retrieves its value if the specified KEY exist. KEY means the name of the file.
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
				peerRequest.setRequestType("LOOKUP");
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
	 * This method searches for the file in the replica i.e. locates the KEY on the Replication Nodes 
	 * in case it is not able to connect to the original peer having that (KEY,VALUE) pair.
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
		
		//System.out.println("Searching Replica - " + peerAddress + ", key - " + key);
		for (String nodeAddress : replicationNodes) {
			try {
				// Checking if the (KEY, VALUE) pair is on our own peer/node
				if (nodeAddress.equals(localAddress)) {
					//System.out.println("\nIf Replica SERVER " + DistributedHashTable.getHashTable() + "\n" + DistributedHashTable.getReplicatedHashTable() + "\n");
					value = FileTransferSystem.getFromReplicaHashTable(key);
				} else {
					//System.out.println("\nElse Replica SERVER " + DistributedHashTable.getHashTable() + "\n" + DistributedHashTable.getReplicatedHashTable() + "\n");
					
					// Make connection with server using the specified Host Address and Port portAddress
			        socket = new Socket(nodeAddress, portAddress);
			        
			        // Initializing output stream using the socket's output stream
			        out = new ObjectOutputStream(socket.getOutputStream());
			        out.flush();
			        
			        // Initializing input stream using the socket's input stream
			        in = new ObjectInputStream(socket.getInputStream());

					// Setup a Request object with Request Type = PUT and Request Data = KEY,VALUE
					peerRequest = new Request();
					peerRequest.setRequestType("R_LOOKUP");
					peerRequest.setRequestData(key);
					out.writeObject(peerRequest);
					
			        // Read the response message from the server
			        serverResponse = (Response) in.readObject();
			        
			        if (serverResponse.getResponseCode() == 200) {
						value = serverResponse.getResponseData().toString().split(",")[1].trim();
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
	 * This methods unregisters a file from the network i.e. deletes a (KEY,VALUE) pair from the Distributed Hash Table (DHT) using KEY.
	 * It does nothing if the KEY doesn't exist in the DHT.
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
			
			// Make connection with server using the specified Host Address and Port portAddress
	        socket = new Socket(nodeAddress, portAddress);
	        
	        // Initializing output stream using the socket's output stream
	        out = new ObjectOutputStream(socket.getOutputStream());
	        out.flush();
	        
	        // Initializing input stream using the socket's input stream
	        in = new ObjectInputStream(socket.getInputStream());

			// Setup a Request object with Request Type = PUT and Request Data = KEY,VALUE
			peerRequest = new Request();
			peerRequest.setRequestType("UNREGISTER");
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