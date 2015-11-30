import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/***
 * The class FileTransferSystem defines the methods and structures (variables) which maintains the Hash Table in the network.
 */
public class FileTransferSystem {
	// <KEY, VALUE> i.e. <FILE NAME, PEER ADDRESS> Hash Table
	private static ConcurrentHashMap<String, String> hashTable = new ConcurrentHashMap<String, String>();
	// <PEER_IP, <KEY, VALUE>> i.e. <PEER_IP, <FILE NAME, PEER ADDRESS>> Hash Table
	private static ConcurrentHashMap<String, HashMap<String, String>> replicatedHashTable = new ConcurrentHashMap<String, HashMap<String, String>>();
	
	private static HashMap<Integer, String> networkMap = new HashMap<Integer, String>();
	private static ArrayList<String> replicationNodes = new ArrayList<String>();
	private static String filesLocation = null;
	private static String replicaLocation = null;
	
	private static final int PEER_SERVER_PORT = 20000;
	private static final String LOCAL_ADDRESS = NetworkUtility.getLocalAddress();
	
	/**
	 * This methods adds a (KEY,VALUE) i.e. (FILENAME, IP ADDRESS) pair in the Distributed Hash Table (hashTable) if the KEY is not already present.
	 * @param key		KEY should be 24 bytes (12 characters) maximum.
	 * @param value		VALUE should be 1000 bytes (500 characters) maximum.
	 * @param confirm	If confirm = true, then KEY check is not done and the (KEY,VALUE) pair is inserted to the hashTable even if the KEY already exists.
	 * 					In this case, the old value of the KEY is replaced by the new VALUE.
	 * @return			Returns true if KEY is added in the hashTable successfully else returns false if a VALUE with the KEY already exists.
	 */
	public static boolean putInHashTable(String key, String value, boolean confirm) {
		if (confirm || !hashTable.containsKey(key)) {
			hashTable.put(key, value);
			return true;
		} else {
			return false;
		}
	}
	
	/***
	 * This methods retrieves the VALUE of the KEY from the Distributed Hash Table (hashTable).
	 * @param key	KEY which is to be searched in the hashTable.
	 * @return		Returns VALUE for the KEY specified if the KEY exist in the hashTable else returns NULL.
	 */
	public static String getFromHashTable(String key) {
		return hashTable.get(key);
	}
	
	/***
	 * This methods deletes a (KEY,VALUE) pair from the Distributed Hash Table (hashTable) using KEY. It does nothing if the KEY doesn't exist in the hashTable.
	 * @param key	KEY of the (KEY,VALUE) pair which has to be deleted from the hashTable.
	 */
	public static void removeFromHashTable(String key) {
		hashTable.remove(key);
	}
	
	/**
	 * This methods adds a (KEY,VALUE) pair in the Replication Hash Table (replicatedHashTable).
	 * @param nodeAddress	IP address of the peer(node) whose hashTable has to be replicated and kept the (KEY, VALUE) pairs in replicatedHashTable.
	 * @param key		KEY should be 24 bytes (12 characters) maximum.
	 * @param value		VALUE should be 1000 bytes (500 characters) maximum.
	 */
	public static void putInReplicaHashTable(String nodeAddress, String key, String value) {
		if (replicatedHashTable.containsKey(nodeAddress)) {
			HashMap<String, String> innerMap = replicatedHashTable.get(nodeAddress);
			innerMap.put(key, value);
		} else {
			HashMap<String, String> innerMap = new HashMap<String, String>();
			innerMap.put(key, value);
			replicatedHashTable.put(nodeAddress, innerMap);
		}
	}
	
	/***
	 * This methods retrieves the VALUE of the KEY from the Replication Hash Table (replicatedHashTable).
	 * @param key	KEY which is to be searched in the replicatedHashTable.
	 * @return		Returns VALUE for the KEY specified if the KEY exist in the replicatedHashTable else returns NULL.
	 */
	public static String getFromReplicaHashTable(String key) {
		String value = null;
		
		for (Map.Entry<String, HashMap<String, String>> record : replicatedHashTable.entrySet()) {
			HashMap<String, String> innerMap = replicatedHashTable.get(record.getKey().toString());
			value = innerMap.get(key);
			if (value != null) {
				break;
			}
		}
		return value;
	}
	
	/***
	 * This methods deletes a (KEY,VALUE) pair from the Replication Hash Table (replicatedHashTable) using KEY. It does nothing if the KEY doesn't exist in the replicatedHashTable.
	 * @nodeAddress	IP address of the peer(node) whose (KEY, VALUE) pair is to be deleted from the replicatedHashTable.
	 * @param key	KEY of the (KEY,VALUE) pair which has to be deleted from the replicatedHashTable.
	 */
	public static void removeFromReplicaHashTable(String nodeAddress, String key) {
		HashMap<String, String> innerMap = replicatedHashTable.get(nodeAddress);
		if (innerMap != null) {
			innerMap.remove(key);
		}
	}

	/***
	 * This method returns the replicatedHashTable which has the replica of the Hash Tables of all the peers in the network.
	 * @return	Returns replicatedHashTable of type ConcurrentHashMap
	 */
	public static ConcurrentHashMap<String, HashMap<String, String>> getReplicatedHashTable() {
		return replicatedHashTable;
	}

	/***
	 * This method sets the replicatedHashTable which has the replica of the Hash Tables of all the peers in the network.
	 * @param replicatedHashTable	ConcurrentHashMap structure containing IP address and hash table of all the peers in the network.
	 */
	public static void setReplicatedHashTable(ConcurrentHashMap<String, HashMap<String, String>> replicatedHashTable) {
		FileTransferSystem.replicatedHashTable = replicatedHashTable;
	}

	/***
	 * This method returns a list of nodes' IP address which are responsible for replication of hash tables.
	 * @return	Returns an ArrayList containing list of replication nodes.
	 */
	public static ArrayList<String> getReplicationNodes() {
		return replicationNodes;
	}

	/***
	 * This method returns the hashTable which stores the (KEY, VALUE) pairs of the calling Peer.
	 * @return	Returns hashTable of type ConcurrentHashMap
	 */
	public static ConcurrentHashMap<String, String> getHashTable() {
		return hashTable;
	}

	/***
	 * This method sets the hashTable which has the (KEY, VALUE) pairs of the calling Peer.
	 * @param hashTable ConcurrentHashMap containing (KEY, VALUE) pairs of the calling Peer. 
	 */
	public static void setHashTable(ConcurrentHashMap<String, String> hashTable) {
		FileTransferSystem.hashTable = hashTable;
	}
	
	/***
	 * This method returns a Map having (ID, IP ADDRESS) of the peers in the network.
	 * @return	Returns the HashMap which has the ID and IP address of all the Peers in the network.
	 */
	public static HashMap<Integer, String> getNetworkMap() {
		return networkMap;
	}

	/***
	 * This method returns the PORT number on which this application runs.
	 * @return	Returns the Port number which the Server is listening to.
	 */
	public static int getPeerServerPort() {
		return PEER_SERVER_PORT;
	}
	
	/***
	 * This method returns the IP Address of the Peer.
	 * @return	Returns the STRING IP Address of the Peer.
	 */
	public static String getLocalAddress() {
		return LOCAL_ADDRESS;
	}
	
	public static void main(String[] args) throws IOException {
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
			} else {
				System.out.println("Configuration File is not as expected. Cannot run program. Bye...");
				System.exit(0);
			}
			
			// Loading replication nodes IP addresses from configuration file
			peerList = configuration.getProperty("REPLICATION_NODES");
			
			if (peerList != null) {
				String[] peers = peerList.split(",");
				
				for (int i = 0; i < peers.length; i++) {
					if (IPAddressValidator.validate(peers[i].trim())) {
						replicationNodes.add(peers[i].trim());
					} else {
						System.out.println("One of the Replication Peer's IP address in the configuration file is invalid. Exiting...");
						System.exit(0);
					}
				}
			}
			
			// Read Files Location whose files are to be shared
			if (configuration.getProperty("FILES_LOCATION") != null) {
				filesLocation = configuration.getProperty("FILES_LOCATION");
			} else {
				filesLocation = "files/";
			}
			
			// Read Replica Files Location where all files are to be stored for replication purpose
			if (configuration.getProperty("REPLICA_LOCATION") != null) {
				replicaLocation = configuration.getProperty("REPLICA_LOCATION");
			} else {
				replicaLocation = "replica/";
			}
			
			//System.out.println(networkMap);
			//System.out.println(replicationNodes);
		} catch (Exception e) {
			System.out.println("ERROR in Loading Configuration File. Cannot run program. Bye...");
			System.exit(0);
		} finally {
			try {
				if (fileStream != null) {
					fileStream.close();
				}
			} catch (Exception e2) { }
		}
		
		// Start a new Thread which acts as Client on Peer side
		System.out.println("********** PEER CLIENT STARTED **********");
		PeerClient peerClient = new PeerClient();
		peerClient.start();
		
		/**
		 * Peer's server implementation. It runs in an infinite loop listening
		 * on port 20000. When a a file download is requested, it spawns a new
		 * thread to do the servicing and immediately returns to listening.
		 */
		System.out.println("********** PEER SERVER STARTED **********");
		ServerSocket listener = new ServerSocket(PEER_SERVER_PORT);
        try {
            while (true) {
            	PeerServer peerServer = new PeerServer(listener.accept());
               peerServer.start();
            }
        } finally {
            listener.close();
        }
	}

	public static String getFilesLocation() {
		return filesLocation;
	}

	public static String getReplicaLocation() {
		return replicaLocation;
	}
}
