import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Peer {
	
	// myIndexedLoc stores the list of all the locations whose files are registered with the Indexing Server.
	private static List<String> myIndexedLoc = Collections.synchronizedList(new ArrayList<String>());
	private static final int PEER_SERVER_PORT = 20000;
	private static final String REPLICATION_PATH = "replica/";
	
	public static void main(String[] args) throws IOException {
		// Start a new Thread which acts as Client on Peer side
		System.out.println("********** PEER CLIENT STARTED **********");
		new PeerClient().start();
		
		/**
		 * Peer's server implementation. It runs in an infinite loop listening
		 * on port 20000. When a a file download is requested, it spawns a new
		 * thread to do the servicing and immediately returns to listening.
		 */
		System.out.println("********** PEER SERVER STARTED **********");
		ServerSocket listener = new ServerSocket(PEER_SERVER_PORT);
        try {
            while (true) {
                new PeerServer(listener.accept()).start();
            }
        } finally {
            listener.close();
        }
	}
	
	private static class PeerServer extends Thread {
		private Socket socket;
		private LogUtility log = new LogUtility("peer");
		
        public PeerServer(Socket socket) {
            this.socket = socket;
            log.write("File downloading with " + socket.getInetAddress() + " started.");
        }
        
        // Services this thread's peer client by sending the requested file.
		public void run() {
			OutputStream out = null;
			ObjectInputStream in = null;
			BufferedInputStream fileInput = null;
			
			try {
				String clientIp = socket.getInetAddress().getHostAddress();
				log.write("Serving download request for " + clientIp);
				
				in = new ObjectInputStream(socket.getInputStream());
				Request request = (Request) in.readObject();
				
				if (request.getRequestType().equalsIgnoreCase("DOWNLOAD")) {
					String fileName = (String) request.getRequestData();
					String fileLocation = FileUtility.getFileLocation(fileName, myIndexedLoc);
					log.write("Uploding/Sending file " + fileName);
					
					File file = new File(fileLocation + fileName);
					byte[] mybytearray = new byte[(int) file.length()];
					fileInput = new BufferedInputStream(new FileInputStream(file));
					fileInput.read(mybytearray, 0, mybytearray.length);
					out = socket.getOutputStream();
					out.write(mybytearray, 0, mybytearray.length);
					out.flush();
					log.write("File sent successfully.");
				} else if (request.getRequestType().equalsIgnoreCase("REPLICATE_DATA")) {
					ConcurrentHashMap<String, ArrayList<String>> data = (ConcurrentHashMap<String, ArrayList<String>>) request.getRequestData();
					new ReplicationService(data).start();
				} else if (request.getRequestType().equalsIgnoreCase("DELETE_DATA")) {
					ArrayList<String> deleteFiles = (ArrayList<String>) request.getRequestData();
					if (deleteFiles != null) {
						for (String fileName : deleteFiles) {
							File file = new File(REPLICATION_PATH + fileName);
							file.delete();
							file = null;
						}
					}
				}
			} catch (Exception e) {
				log.write("Error in sending file.");
				log.write("ERROR:" + e);
			} finally {
				try {
					// Closing all streams. Close the stream only if it is initialized
					if (out != null)
						out.close();
					
					if (in != null)
						in.close();
					
					if (fileInput != null)
						fileInput.close();
					
					if (socket != null)
						socket.close();
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
				Thread.currentThread().interrupt();
			}
		}

		@Override
		public void interrupt() {
			log.close();
			super.interrupt();
		}
	}
	
	private static class PeerClient extends Thread {
		
		// Thread implementation for Peer to serve as CLient
		public void run() {
			Socket socket = null;
			ObjectInputStream in = null;
			BufferedReader input = null;
			ObjectOutputStream out = null;
			Request peerRequest = null;
			Response serverResponse	= null;
			
			try {
				input = new BufferedReader(new InputStreamReader(System.in));
				System.out.println("Enter Server IP Address:");
		        String serverAddress = input.readLine();
		        long startTime, endTime;
		        double time;
		        
		        if(serverAddress.trim().length() == 0 || !IPAddressValidator.validate(serverAddress)) {
					System.out.println("Invalid Server IP Address.");
					System.exit(0);
				}

		        // Make connection with server using the specified Host Address and Port 10000
		        socket = new Socket(serverAddress, 10000);
		        
		        // Initializing output stream using the socket's output stream
		        out = new ObjectOutputStream(socket.getOutputStream());
		        out.flush();
		        
		        // Initializing input stream using the socket's input stream
		        in = new ObjectInputStream(socket.getInputStream());

		        // Read the initial welcome message from the server
		        serverResponse = (Response) in.readObject();
		        System.out.print((String) serverResponse.getResponseData());
		        String replicaChoice = input.readLine();
		        
		        // Setup a Request object with Request Type = REPLICATION and Request Data = Choice
				peerRequest = new Request();
				peerRequest.setRequestType("REPLICATION");
				peerRequest.setRequestData(replicaChoice);
				out.writeObject(peerRequest);
				
				if (replicaChoice.equalsIgnoreCase("Y")) {
					// Read the Replication response from the server
					myIndexedLoc.add(REPLICATION_PATH);
					serverResponse = (Response) in.readObject();
					ConcurrentHashMap<String, ArrayList<String>> data = (ConcurrentHashMap<String, ArrayList<String>>) serverResponse.getResponseData();
					new ReplicationService(data).start();
				}
				
				// Previously indexed locations if any
				serverResponse = (Response) in.readObject();
				ArrayList<String> indexedLocations =  (ArrayList<String>) serverResponse.getResponseData();
				if (indexedLocations != null) {
					for (String x : indexedLocations) {
						if (!myIndexedLoc.contains(x)) {
							myIndexedLoc.add(x);
						}
					}
				}
				
		        while (true) {
		        	// Display different choices to the user
		        	System.out.println("\nWhat do you want to do?");
			        System.out.println("1.Register files with indexing server.");
			        System.out.println("2.Lookup for a file at index server.");
			        System.out.println("3.Un-register all files of this peer from the indexing server.");
			        System.out.println("4.Print download log of this peer.");
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
			        // Register files with indexing server functionality
					case 1:
						System.out.println("\nEnter path of the files to sync with indexing server:");
						String path = input.readLine();
						
						// Checking if the user has entered something
						if(path.trim().length() == 0) {
							System.out.println("Invalid Path.");
							continue;
						}
						
						// Retrieve all the files from the user's specified location
						ArrayList<String> files = FileUtility.getFiles(path);
						
						// Add the user's entered file/path to peer's indexed location's list
						File file = new File(path);
						if (file.isFile()) {
							myIndexedLoc.add(path.substring(0, path.lastIndexOf("/")));
							System.out.println(path.substring(0, path.lastIndexOf("/")));
							files.add(0, path.substring(0, path.lastIndexOf("/")));
						} else if (file.isDirectory()) {
							myIndexedLoc.add(path);
							files.add(0, path);
						}
						
						// 1 because path is always there
						if (files.size() > 1) {
							startTime = System.currentTimeMillis();

							// Setup a Request object with Request Type = REGISTER and Request Data = files array list
							peerRequest = new Request();
							peerRequest.setRequestType("REGISTER");
							peerRequest.setRequestData(files);
							out.writeObject(peerRequest);
							
							// Retrieve response from the server
							serverResponse = (Response) in.readObject();
							endTime = System.currentTimeMillis();
							time = (double) Math.round(endTime - startTime) / 1000;
							
							// If Response is success i.e. Response Code = 200, then print success message else error message
							if (serverResponse.getResponseCode() == 200) {
								/*indexedLocations =  (ArrayList<String>) serverResponse.getResponseData();
								for (String x : indexedLocations) {
									if (!myIndexedLoc.contains(x)) {
										myIndexedLoc.add(x);
									}
								}*/
								System.out.println((files.size() - 1) + " files registered with indexing server. Time taken:" + time + " seconds.");
							} else {
								System.out.println("Unable to register files with server. Please try again later.");
							}
						} else {
							System.out.println("0 files found at this location. Nothing registered with indexing server.");
						}
						break;

					// Handling file lookup on indexing server functionality
					case 2:
						System.out.println("\nEnter name of the file you want to look for at indexing server:");
						String fileName = input.readLine();
						String hostAddress;
						
						startTime = System.currentTimeMillis();
						// Setup a Request object with Request Type = LOOKUP and Request Data = file to be searched
						peerRequest = new Request();
						peerRequest.setRequestType("LOOKUP");
						peerRequest.setRequestData(fileName);
						out.writeObject(peerRequest);
						
						serverResponse = (Response) in.readObject();
						endTime = System.currentTimeMillis();
						time = (double) Math.round(endTime - startTime) / 1000;
						
						// If Response is success i.e. Response Code = 200, then perform download operation else error message
						if (serverResponse.getResponseCode() == 200) {
							System.out.println("File Found. Lookup time: " + time + " seconds.");
							
							// Response Data contains the List of Peers which contain the searched file
							HashMap<Integer, String> lookupResults = (HashMap<Integer, String>) serverResponse.getResponseData();
							
							// Printing all Peer details that contain the searched file
							if (lookupResults != null) {
								for (Map.Entry e : lookupResults.entrySet()) {
									System.out.println("\nPeer ID:" + e.getKey().toString());
									System.out.println("Host Address:" + e.getValue().toString());
								}
							}
							
							// If the file is a Text file then we can print or else only download file
							if (fileName.trim().endsWith(".txt")) {
								System.out.print("\nDo you want to download (D) or print this file (P)? Enter (D/P):");
								String download = input.readLine();
								
								// In case there are more than 1 peer, then we user will select which peer to use for download
								if(lookupResults.size() > 1) {
									System.out.print("Enter Peer ID from which you want to download the file:");
									int peerId = Integer.parseInt(input.readLine());
									hostAddress = lookupResults.get(peerId);
								} else {
									Map.Entry<Integer,String> entry = lookupResults.entrySet().iterator().next();
									hostAddress = entry.getValue();
								}
								
								if (download.equalsIgnoreCase("D")) {
									System.out.println("The file will be downloaded in the 'downloads' folder in the current location.");
									// Obtain the searched file from the specified Peer
									obtain(hostAddress, 20000, fileName, out, in);
								} else if (download.equalsIgnoreCase("P")) {
									// Obtain the searched file from the specified Peer and print its contents
									obtain(hostAddress, 20000, fileName, out, in);
									FileUtility.printFile(fileName);
								}
							} else {
								System.out.print("\nDo you want to download this file?(Y/N):");
								String download = input.readLine();
								if (download.equalsIgnoreCase("Y")) {
									if(lookupResults.size() > 1) {
										System.out.print("Enter Peer ID from which you want to download the file:");
										int peerId = Integer.parseInt(input.readLine());
										hostAddress = lookupResults.get(peerId);
									} else {
										Map.Entry<Integer,String> entry = lookupResults.entrySet().iterator().next();
										hostAddress = entry.getValue();
									}
									// Obtain the searched file from the specified Peer
									obtain(hostAddress, 20000, fileName, out, in);
								}	
							}					
						} else {
							System.out.println((String) serverResponse.getResponseData());
							System.out.println("Lookup time: " + time + " seconds.");
						}
						break;
						
					// Handling de-registration of files from the indexing server
					case 3:
						// Confirming user's un-register request
						System.out.print("\nAre you sure (Y/N)?:");
						String confirm = input.readLine();
						
						if (confirm.equalsIgnoreCase("Y")) {
							startTime = System.currentTimeMillis();
							// Setup a Request object with Request Type = UNREGISTER and Request Data = general message
							peerRequest = new Request();
							peerRequest.setRequestType("UNREGISTER");
							peerRequest.setRequestData("Un-register all files from index server.");
							out.writeObject(peerRequest);
							endTime = System.currentTimeMillis();
							time = (double) Math.round(endTime - startTime) / 1000;
							
							serverResponse = (Response) in.readObject();
							System.out.println((String) serverResponse.getResponseData());
							System.out.println("Time taken:" + time + " seconds.");
						}
						break;
						
					// Printing the download log
					case 4:
						(new LogUtility("peer")).print();
						break;
						
					// Handling Peer exit functionality
					case 5:
						// Setup a Request object with Request Type = DISCONNECT and Request Data = general message
						peerRequest = new Request();
						peerRequest.setRequestType("DISCONNECT");
						peerRequest.setRequestData("Disconnecting from server.");
						out.writeObject(peerRequest);
						System.out.println("Thanks for using this system.");
						System.exit(0);
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
					if (out != null)
						out.close();
					
					if (in != null)
						in.close();
					
					if (socket != null)
						socket.close();
					
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
		private void obtain(String hostAddress, int port, String fileName, ObjectOutputStream out, ObjectInputStream in) {
			boolean isDownloaded = false;
			long startTime = System.currentTimeMillis();
			
			if (!FileUtility.downloadFile(hostAddress, port, fileName)) {
				try {
					Request peerRequest = new Request();
					peerRequest.setRequestType("GET_BACKUP_NODES");
					peerRequest.setRequestData("Send list of backup nodes.");
					out.writeObject(peerRequest);
				
					Response serverResponse = (Response) in.readObject();
					List<String> backupNodes = (List<String>) serverResponse.getResponseData();
					
					//System.out.println(backupNodes);
					for (String node : backupNodes) {
						if(FileUtility.downloadFile(node, port, fileName)) {
							isDownloaded = true;
							break;
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
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
	}
	
	/***
	 * This class acts as a replicator. The main function of this class is to
	 * grab files from all the peers and store it in its replication directory.
	 * When the Peer responds to server that it is ready to act as the
	 * replication node, a new thread is created which is the replication
	 * service thread which performs this task. It uses the same request format
	 * to request a file from the peer as the Peer Client does to request a file
	 * from another peer. A different thread is created so that the replicator
	 * service doesn't affect other operations.
	 */
	private static class ReplicationService extends Thread {
		private static ConcurrentHashMap<String, ArrayList<String>> data = new ConcurrentHashMap<String, ArrayList<String>>();
		
		public ReplicationService (ConcurrentHashMap<String, ArrayList<String>> data) {
			ReplicationService.data = data;
		}
		
		public void run () {
			for (Map.Entry e : data.entrySet()) {
				String key = e.getKey().toString();
				ArrayList<String> value = (ArrayList<String>) e.getValue();
				String hostAddress = key.split("#")[1].trim();
				for (String file : value) {
					// Replicate file from the respective peer
					replicate(hostAddress, 20000, file);
				}
			}
			this.interrupt();
		}
		
		/***
		 * This method is used to download the file from the requested Peer.
		 * @param hostAddress 	IP Address of the peer used to download the file
		 * @param port			Port of the per used to download the file
		 * @param fileName		Name of the file to be downloaded
		 */
		private void replicate(String hostAddress, int port, String fileName) {
			FileUtility.replicateFile(hostAddress, port, fileName);
		}
	}
}
