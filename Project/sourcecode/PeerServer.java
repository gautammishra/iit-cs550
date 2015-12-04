import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;

public class PeerServer extends Thread {	
	private Socket socket;
	//private LogUtility log = null;
	
	// Initialize all the local data from the global data
	public PeerServer(Socket socket) {
		this.socket = socket;
		
		//log = new LogUtility("peer");
		////log.write("Connected with " + socket.getInetAddress() + ".");
	}
    
    // Services this thread's peer client by sending the requested file.
	public void run() {	
		ObjectOutputStream out = null;
		ObjectInputStream in = null;
		Response response = null;
		
		try {
			String clientIp = socket.getInetAddress().getHostAddress();
			
			in = new ObjectInputStream(socket.getInputStream());
			out = new ObjectOutputStream(socket.getOutputStream());
			out.flush();
			
			Request request = (Request) in.readObject();
			
			if (request.getRequestType().startsWith("PUT")) {
				String data = (String) request.getRequestData();
				String key = data.split(",")[0];
				String value = data.split(",")[1];
				boolean result;
				
				////log.write(String.format("Serving PUT(%s,%s) request of %s.", key, value, clientIp));
				
				if (request.getRequestType().endsWith("FORCE")) {
					result = DistributedHashTable.putInHashTable(key, value, true);
				} else {
					result = DistributedHashTable.putInHashTable(key, value, false);
				}
				
				if (result) {
					response = new Response();
					response.setResponseCode(200);
					response.setResponseData("(Key,Value) pair added successfully.");
					out.writeObject(response);
					
					//log.write(String.format("PUT(%s,%s) for %s completed successfully.", key, value, clientIp));
					
					ReplicationService service = new ReplicationService(key, value, "PUT");
					service.start();
				} else {
					response = new Response();
					response.setResponseCode(300);
					response.setResponseData("Value with this KEY already exist.");
					out.writeObject(response);
					
					//log.write(String.format("PUT(%s,%s) for %s failed. KEY already exist.", key, value, clientIp));
				}
			} else if (request.getRequestType().equalsIgnoreCase("GET")) {
				String key = (String) request.getRequestData();
				
				//log.write(String.format("Serving GET(%s) request of %s.", key, clientIp));
				String value = DistributedHashTable.getFromHashTable(key);
				
				if (value != null) {
					response = new Response();
					response.setResponseCode(200);
					response.setResponseData(key + "," + value);
					out.writeObject(response);
					//log.write(String.format("GET(%s) = %s for %s completed successfully.", key, value, clientIp));
				} else {
					response = new Response();
					response.setResponseCode(404);
					response.setResponseData("VALUE with this KEY does not exist.");
					out.writeObject(response);
					//log.write(String.format("GET(%s) = %s for %s completed successfully. Key not found.", key, value, clientIp));
				}
			} else if (request.getRequestType().equalsIgnoreCase("DELETE")) {					
				String key = (String) request.getRequestData();
				
				//log.write(String.format("Serving DELETE(%s) request of %s.", key, clientIp));
				DistributedHashTable.removeFromHashTable(key);

				response = new Response();
				response.setResponseCode(200);
				out.writeObject(response);
				
				//log.write(String.format("DELETE(%s) for %s completed successfully.", key, clientIp));
				
				ReplicationService service = new ReplicationService(key, null, "DELETE");
				service.start();
			} else if (request.getRequestType().equalsIgnoreCase("R_PUT")) {
				String data = (String) request.getRequestData();
				String key = data.split(",")[0];
				String value = data.split(",")[1];
		
				//System.out.println("\nR_PUT replicatedHashTable = " + DistributedHashTable.getReplicatedHashTable());
				//log.write(String.format("Serving REPLICATE - PUT(%s,%s) request of %s.", key, value, clientIp));
				DistributedHashTable.putInReplicaHashTable(clientIp, key, value);
				
				//System.out.println("\nR_PUT replicatedHashTable = " + DistributedHashTable.getReplicatedHashTable());
				response = new Response();
				response.setResponseCode(200);
				response.setResponseData("(Key,Value) pair added successfully.");
				out.writeObject(response);
				
				//log.write(String.format("REPLICATE - PUT(%s,%s) for %s completed successfully.", key, value, clientIp));
			} else if (request.getRequestType().equalsIgnoreCase("R_GET")) {
				String key = (String) request.getRequestData();
				
				//log.write(String.format("Serving REPLICATE - GET(%s) request of %s.", key, clientIp));
				String value = null;
				
				value = DistributedHashTable.getFromReplicaHashTable(key);
				
				if (value != null) {
					response = new Response();
					response.setResponseCode(200);
					response.setResponseData(key + "," + value);
					out.writeObject(response);
					//log.write(String.format("REPLICATE - GET(%s) = %s for %s completed successfully.", key, value, clientIp));
				} else {
					response = new Response();
					response.setResponseCode(404);
					response.setResponseData("VALUE with this KEY does not exist.");
					out.writeObject(response);
					//log.write(String.format("REPLICATE - GET(%s) = %s for %s completed successfully. Key not found.", key, value, clientIp));
				}
			} else if (request.getRequestType().equalsIgnoreCase("R_DELETE")) {					
				String key = (String) request.getRequestData();
				
				//log.write(String.format("Serving REPLICATE - DELETE(%s) request of %s.", key, clientIp));
				
				DistributedHashTable.removeFromReplicaHashTable(clientIp, key);

				response = new Response();
				response.setResponseCode(200);
				out.writeObject(response);
				
				//log.write(String.format("REPLICATE - DELETE(%s) for %s completed successfully.", key, clientIp));
			} else if (request.getRequestType().equalsIgnoreCase("GET_HASHTABLE")) {					
				//log.write(String.format("Serving GET_HASHTABLE request of %s.", clientIp));
				
				//System.out.println(DistributedHashTable.getReplicatedHashTable());
				HashMap<String, String> innerMap = DistributedHashTable.getReplicatedHashTable().get(clientIp);
				
				if (innerMap != null) {
					response = new Response();
					response.setResponseCode(200);
					response.setOtherData(innerMap);
					out.writeObject(response);
				} else {
					response = new Response();
					response.setResponseCode(404);
					out.writeObject(response);
				}
				
				//log.write(String.format("DATA of %s sent successfully. Request completed. " + innerMap, clientIp));
			} else if (request.getRequestType().equalsIgnoreCase("GET_R_HASHTABLE")) {					
				//log.write(String.format("Serving GET_R_HASHTABLE request of %s.", clientIp));
				//System.out.println("Sending Replication Hash Table = " + DistributedHashTable.getReplicatedHashTable());
				
				response = new Response();
				response.setResponseCode(200);
				response.setOtherData(DistributedHashTable.getReplicatedHashTable());
				out.writeObject(response);
				
				//log.write(String.format("REPLCATION DATA sent to %s successfully. Request completed. " + DistributedHashTable.getReplicatedHashTable(), clientIp));
			}
		} catch (Exception e) {
			////log.write("ERROR:" + e);
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
			} catch (IOException ioe) {
				//ioe.printStackTrace();
			}
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public void interrupt() {
		//log.close();
		super.interrupt();
	}
}