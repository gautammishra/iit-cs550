import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The indexing server program accepts request from peers to register/unregister
 * files and search a file int its indexing database.
 */
public class IndexingServer {

	/***
	 * indexDatabase is a concurrent hash map used to store the records which contains all the files registered from various Peers.
	 * We are using ConcurrentHasMap because it is serializable as well as thread safe.
	 * The key of the indexDB contains Peer ID and Peer IP Address separated by # (Example: 1#127.0.0.1)
	 * The value of the indexDB contains an ArrayList of String which contains the list of files.
	 */
	private static ConcurrentHashMap<String, ArrayList<String>> indexDatabase = new ConcurrentHashMap<String, ArrayList<String>>();
	private static ConcurrentHashMap<String, ArrayList<String>> peerIndexedLocations = new ConcurrentHashMap<String, ArrayList<String>>();
	private static List<String> replicationNodes = Collections.synchronizedList(new ArrayList<String>());
	private static final int SERVER_SOCKET_PORT = 10000; 
	private static final int PEER_SERVER_PORT = 20000;
	private static final String REPLICA_LOCATION = "replica/";
	
	// totalPeers stores the count of peers connected to the indexing server
	private static int totalPeers = 0;
	
	/**
	 * Indexing Server's main method to run the server. It runs in an infinite
	 * loop listening on port 10000. When a connection is requested, it spawns a
	 * new thread to do the servicing and immediately returns to listening. The
	 * server keeps a unique peer id for each peer that connects to the server
	 * for file sharing.
	 */
    public static void main(String[] args) throws Exception {
        System.out.println("********** INDEXING SERVER STARTED **********");
        int peerId = 1;
        
        ServerSocket listener = new ServerSocket(SERVER_SOCKET_PORT);
        try {
            while (true) {
                new Indexer(listener.accept(), peerId++).start();
            }
        } finally {
            listener.close();
        }
    }

    // A private thread to handle peer's file sharing requests on a particular socket.
    private static class Indexer extends Thread {
        private Socket socket;
        private int clientNumber;
        
        public Indexer(Socket socket, int clientNumber) {
            this.socket = socket;
            this.clientNumber = clientNumber;
            print("\nNew connection with Peer # " + clientNumber + " at " + socket.getInetAddress());
            totalPeers++;
            print("Total number of peers connected:" + totalPeers);
        }

		/**
		 * Services this thread's client by first sending the client a welcome
		 * message then repeatedly reading requests from the peer.
		 */
        public void run() {
            try {
            	// Initializing output stream using the socket's output stream
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                
                // Initializing input stream using the socket's input stream
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                String clientIp = socket.getInetAddress().getHostAddress();

                // Send a welcome message to the client
                Response response = new Response();
                response.setResponseCode(200);
                response.setResponseData("Hello, you are Peer #" + clientNumber + ".\nDo you want your node to act as a replication node? This requires your disk space to be large. (Y/N):");
                out.writeObject(response);
                
                Request peerRequest = (Request) in.readObject();
                String requestType = peerRequest.getRequestType();
                String replicaChoice = (String) peerRequest.getRequestData();
                
                if (replicaChoice.equalsIgnoreCase("Y")) {
                	System.out.println("Replication with this node accepted.");
                	
                	if (!replicationNodes.contains(clientIp)) {
                		replicationNodes.add(clientIp);
					}
                	
                	// Just to remind peer if he is acting as a replication node
                	if (peerIndexedLocations.containsKey(clientIp)) {
        				peerIndexedLocations.get(clientIp).add(REPLICA_LOCATION);
        			} else {
        				ArrayList<String> paths = new ArrayList<String>();
        	        	paths.add(REPLICA_LOCATION);
        	        	peerIndexedLocations.put(clientIp, paths);
        			}
                	
                	response = new Response();
                	response.setResponseCode(200);
                    response.setResponseData(indexDatabase);
                    out.writeObject(response);
				}
                
                response = new Response();
                response.setResponseCode(200);
                response.setResponseData(peerIndexedLocations.get(clientIp));
                out.writeObject(response);
                
                while(true) {
                	// Read the request object received from the Peer
                	peerRequest = (Request) in.readObject();
                    requestType = peerRequest.getRequestType();
                    
                    if (requestType.equalsIgnoreCase("REGISTER")) {
                    	// If Request Type = REGISTER, then call register(...) method to register the peer's files
                    	ArrayList<String> indexedLocations = register(clientNumber, clientIp, (ArrayList<String>) peerRequest.getRequestData(), out);
                    	response = new Response();
                    	response.setResponseCode(200);
                        response.setResponseData(indexedLocations);
                        out.writeObject(response);
    				} else if (requestType.equalsIgnoreCase("LOOKUP")) {
    					print("\nLooking up a file.");
    					String fileName = (String) peerRequest.getRequestData();
    					
    					// If Request Type = LOOKUP, then call search(...) method to search for the specified file
    					print("Request from Peer # " + clientNumber + " (" + clientIp + ") to look for file " + fileName);
    					HashMap<Integer, String> searchResults = search(fileName);
    					
    					// If file found then respond with all the peer locations that contain the file or else send File Not Found message
    					if (searchResults.size() > 0) {
    						response = new Response();
    						response.setResponseCode(200);
    						response.setResponseData(searchResults);
    						out.writeObject(response);
    						print("File Found.");
    					} else {
    						response = new Response();
    						response.setResponseCode(404);
    						response.setResponseData("File Not Found.");
    						out.writeObject(response);
    						print("File Not Found.");
    					}
    				} else if(requestType.equalsIgnoreCase("UNREGISTER")) {
    					// If Request Type = UNREGISTER, then call unregister(...) method to remove all the files of the requested 
    					// peer from the indexing server's database
    					response = new Response();
    					if (unregister(clientIp)) {
    						response.setResponseCode(200);
    						response.setResponseData("Your files have been un-registered from the indexing server.");
    						print("Peer # " + clientNumber + " (" + clientIp + ") has un-registered all its files.");
						} else {
							response.setResponseCode(400);
							response.setResponseData("Error in un-registering files from the indexing server.");
						}
						out.writeObject(response);
    				} else if(requestType.equalsIgnoreCase("GET_BACKUP_NODES")) {
    					// Sends replication peers/nodes to the peer who is not able to download a file from its original peer.
    					System.out.println("\n" + clientIp + " requested backup nodes info. Sending backup nodes info.");
    					response = new Response();
    					response.setResponseCode(200);
						response.setResponseData(replicationNodes);
						out.writeObject(response);
						System.out.println("Backup nodoes information sent.");
    				} else if(requestType.equalsIgnoreCase("DISCONNECT")) {
    					print("\nPeer # " + clientNumber + " disconnecting...");
    					try {
    						// Close the connection and then stop the thread.
    	                    socket.close();
    	                } catch (IOException e) {
    	                    print("Couldn't close a socket.");
    	                }
    	                Thread.currentThread().interrupt();
    	                break;
    				}
                }
            } catch(EOFException e) {
            	Thread.currentThread().interrupt();
            } catch (Exception e) {
                print("Error handling Peer # " + clientNumber + ": " + e);
                Thread.currentThread().interrupt();
            }
        }
        
        // Stop thread once the peer has disconnected or some error has occurred in serving the peer.
        public void interrupt() {
        	print("\nConnection with Peer # " + clientNumber + " closed");
        	totalPeers--;
        	print("Total number of peers connected:" + totalPeers);
        	if (totalPeers == 0) {
        		print("No more peers connected.");
			}
        }

        /***
         * This method prints the message.
         * @param message Message to be printed on the console screen
         */
        private void print(String message) {
        	LogUtility log = new LogUtility("server");
        	log.write(message);
        	log.close();
            System.out.println(message);
        }
        
        /***
         * This method registers the files sent by the peer.
         * @param peerId		ID of the Peer who wants to register its files with the indexing server
         * @param peerAddress	IP Address of the Peer who wants to register its files with the indexing server
         * @param files			List of files to be registered with the indexing server
         */
        private ArrayList<String> register(int peerId, String peerAddress, ArrayList<String> files, ObjectOutputStream out) throws IOException {
        	print("\nRegistering files from Peer " + peerAddress);
        	
        	// Appending HHmmss just to make the key unique because a single peer may register multiple times. We aren't using the last appended data.
        	String time = new SimpleDateFormat("HHmmss").format(Calendar.getInstance().getTime());
        	
        	// Retrieving path and storing them separately
        	if (peerIndexedLocations.containsKey(peerAddress)) {
				peerIndexedLocations.get(peerAddress).add(files.get(0));
			} else {
				ArrayList<String> paths = new ArrayList<String>();
	        	paths.add(files.get(0));
	        	peerIndexedLocations.put(peerAddress, paths);
			}
        	files.remove(0);
        	
        	// Using StringBuffer to avoid creation of multiple string objects while appending
        	StringBuffer sb = new StringBuffer();
        	sb.append(clientNumber).append("#").append(peerAddress).append("#").append(time);
            indexDatabase.put(sb.toString(), files);
            
            print(files.size() + " files synced with Peer " + clientNumber + " and added to index database");
            
            ConcurrentHashMap<String, ArrayList<String>> newFiles = new ConcurrentHashMap<String, ArrayList<String>>();
            newFiles.put(sb.toString(), files);
            sendReplicateCommand(newFiles);
            
            return peerIndexedLocations.get(peerAddress);
        }
        
        /***
         * This methods removes the file entries of the requested peer from the indeexDB
         * @param peerAddress	IP Address of the peer whose files are to be removed from the indexing server's database
         * @return				Returns true if operation is successful else false
         */
        private boolean unregister(String peerAddress) throws IOException {
        	int oldSize = indexDatabase.size();
        	ArrayList<String> deleteFiles = null;
        	
        	for (Map.Entry e : indexDatabase.entrySet()) {
				String key = e.getKey().toString();
				ArrayList<String> value = (ArrayList<String>) e.getValue();
				
				if (key.contains(peerAddress)) {
					deleteFiles = indexDatabase.get(key);
					indexDatabase.remove(key);
				}
			}
        	int newSize = indexDatabase.size();
        	
        	
        	// Send request to delete the unregistered files from the replication node
        	if (newSize < oldSize) {
        		Request serverRequest = new Request();
            	Socket socket = null;
            	try {
            		serverRequest.setRequestType("DELETE_DATA");
                	for (String node : replicationNodes) {
                		socket = new Socket(node, PEER_SERVER_PORT);
                		ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        				serverRequest.setRequestData(deleteFiles);
        				out.writeObject(serverRequest);
        				out.close();
        				socket.close();
        			}
                	socket = null;
    			} catch (Exception e) {
    				print("Error in replication:" + e);
    			} finally {
    				serverRequest = null;
    				if (socket != null && socket.isConnected()) {
    					socket.close();
    				}
    			}
			}
        	
        	return (newSize < oldSize);
        }
        
        /***
         * This method searches the specified fileName in the indexing server's database
         * @param fileName	Name of the file to be searched in the indexing server's database
         * @return			Returns a hashmap which contains <Peer ID, Peer IP Address> of all the peers that contain the searched file
         */
        private HashMap<Integer, String> search(String fileName) {
        	HashMap<Integer, String> searchResults = new HashMap<Integer, String>();
			for (Map.Entry e : indexDatabase.entrySet()) {
				String key = e.getKey().toString();
				ArrayList<String> value = (ArrayList<String>) e.getValue();
				
				for (String file : value) {
					if (file.equalsIgnoreCase(fileName)) {
						int peerId = Integer.parseInt(key.split("#")[0].trim());
						String hostAddress = key.split("#")[1].trim();
						searchResults.put(peerId, hostAddress);
					}
				}
			}
			return searchResults;
        }
        
        /***
         * This method is called whenever a peer registers its files.
         * The Indexing server sends a REPLICATE_DATA to the replication nodes to update its replication data.
         * @param newFiles	HashMap contaning peer address and list of new files which has been registered by the Peer
         */
        private void sendReplicateCommand(ConcurrentHashMap<String, ArrayList<String>> newFiles) throws IOException {
        	Request serverRequest = new Request();
        	Socket socket = null;
        	try {
        		serverRequest.setRequestType("REPLICATE_DATA");
            	for (String node : replicationNodes) {
            		socket = new Socket(node, PEER_SERVER_PORT);
            		ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
    				serverRequest.setRequestData(newFiles);
    				out.writeObject(serverRequest);
    				out.close();
    				socket.close();
    			}
            	socket = null;
			} catch (Exception e) {
				print("Error in replication:" + e);
			} finally {
				serverRequest = null;
				if (socket != null && socket.isConnected()) {
					socket.close();
				}
			}
        }
    }
}