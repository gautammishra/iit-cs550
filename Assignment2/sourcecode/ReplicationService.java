import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

/***
 * This class provides the Replication facility in the Distributed Hash Table.
 * Its serves the requests of peers to store their Hash Table data on the replication nodes.
 */
public class ReplicationService extends Thread {
	private ArrayList<String> replicationNodes = null;
	private int portAddress;
	private String localAddress = null;

	private String key = null;
	private String value = null;
	private String requestType = null;
	
	// Initialize all the local data from the global data
	public ReplicationService(String key, String value, String requestType) {
		replicationNodes = DistributedHashTable.getReplicationNodes();
		portAddress = DistributedHashTable.getPeerServerPort();
		localAddress = DistributedHashTable.getLocalAddress();
		
		this.key = key;
		this.value = value;
		this.requestType = requestType;
	}
	
	public void run () {
		Socket socket = null;
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		Request peerRequest = null;
		Response serverResponse	= null;
		String data = key + "," + value;
		
		for (String nodeAddress : replicationNodes) {
			try {
				if (requestType.equalsIgnoreCase("PUT")) {
					if (nodeAddress.equalsIgnoreCase(localAddress)) {
						//System.out.println(String.format("\nREPLICATING If (%s,%s) at %s - %s", key, value, nodeAddress, localAddress));
						DistributedHashTable.putInReplicaHashTable(nodeAddress, key, value);
					} else {
						//System.out.println(String.format("\nREPLICATING Else (%s,%s) at %s - %s", key, value, nodeAddress, localAddress));
						
						// Make connection with server using the specified Host Address and Port 10000
						socket = new Socket(nodeAddress, portAddress);

						// Initializing output stream using the socket's output stream
						out = new ObjectOutputStream(socket.getOutputStream());
						out.flush();

						// Initializing input stream using the socket's input stream
						in = new ObjectInputStream(socket.getInputStream());

						// Setup a Request object with Request Type = PUT and Request Data = KEY,VALUE
						peerRequest = new Request();
						peerRequest.setRequestType("R_PUT");
						peerRequest.setRequestData(data);
						out.writeObject(peerRequest);

						// Read the response message from the server
						serverResponse = (Response) in.readObject();
						socket.close();
					}
					//System.out.println(replicatedHashTable);
				} else if (requestType.equalsIgnoreCase("DELETE")) {
					// Make connection with server using the specified Host Address and Port 10000
			        socket = new Socket(nodeAddress, portAddress);
			        
			        // Initializing output stream using the socket's output stream
			        out = new ObjectOutputStream(socket.getOutputStream());
			        out.flush();
			        
			        // Initializing input stream using the socket's input stream
			        in = new ObjectInputStream(socket.getInputStream());

					// Setup a Request object with Request Type = PUT and Request Data = KEY,VALUE
					peerRequest = new Request();
					peerRequest.setRequestType("R_DELETE");
					peerRequest.setRequestData(key);
					out.writeObject(peerRequest);
					
			        // Read the response message from the server
			        serverResponse = (Response) in.readObject();
			        socket.close();
				}
			} catch (Exception ex) {
				// e.printStackTrace();
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
		this.interrupt();
	}
}