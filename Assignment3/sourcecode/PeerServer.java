import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.util.HashMap;

public class PeerServer extends Thread {
	
	private Socket socket = null;
	private LogUtility log = null;
	private String filesLocation = null;
	private int portAddress;
	private String replicaLocation = null;
	
	// Initialize all the local data from the global data
	public PeerServer(Socket socket) {
		this.socket = socket;
		
		log = new LogUtility("peer");
		log.write("Connected with " + socket.getInetAddress() + ".");
		filesLocation = FileTransferSystem.getFilesLocation();
		portAddress = FileTransferSystem.getPeerServerPort();
		replicaLocation = FileTransferSystem.getReplicaLocation();
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
			
			if (request.getRequestType().startsWith("REGISTER")) {
				String data = (String) request.getRequestData();
				String key = data.split(",")[0];
				String value = data.split(",")[1];
				boolean result;
				
				log.write(String.format("Serving REGISTER(%s,%s) request of %s.", key, value, clientIp));
				
				if (request.getRequestType().endsWith("FORCE")) {
					result = FileTransferSystem.putInHashTable(key, value, true);
				} else {
					result = FileTransferSystem.putInHashTable(key, value, false);
				}
				
				if (result) {
					response = new Response();
					response.setResponseCode(200);
					response.setResponseData("(Key,Value) pair added successfully.");
					out.writeObject(response);
					
					log.write(String.format("REGISTER(%s,%s) for %s completed successfully.", key, value, clientIp));
					
					ReplicationService service = new ReplicationService(key, value, "REGISTER");
					service.start();
				} else {
					response = new Response();
					response.setResponseCode(300);
					response.setResponseData("Value with this KEY already exist.");
					out.writeObject(response);
					
					log.write(String.format("REGISTER(%s,%s) for %s failed. KEY already exist.", key, value, clientIp));
				}
			} else if (request.getRequestType().equalsIgnoreCase("LOOKUP")) {
				String key = (String) request.getRequestData();
				
				log.write(String.format("Serving LOOKUP(%s) request of %s.", key, clientIp));
				String value = FileTransferSystem.getFromHashTable(key);
				
				if (value != null) {
					response = new Response();
					response.setResponseCode(200);
					response.setResponseData(key + "," + value);
					out.writeObject(response);
					log.write(String.format("LOOKUP(%s) = %s for %s completed successfully.", key, value, clientIp));
				} else {
					response = new Response();
					response.setResponseCode(404);
					response.setResponseData("VALUE with this KEY does not exist.");
					out.writeObject(response);
					log.write(String.format("LOOKUP(%s) = %s for %s completed successfully. Key not found.", key, value, clientIp));
				}
			} else if (request.getRequestType().equalsIgnoreCase("UNREGISTER")) {					
				String key = (String) request.getRequestData();
				
				log.write(String.format("Serving UNREGISTER(%s) request of %s.", key, clientIp));
				FileTransferSystem.removeFromHashTable(key);

				response = new Response();
				response.setResponseCode(200);
				out.writeObject(response);
				
				log.write(String.format("UNREGISTER(%s) for %s completed successfully.", key, clientIp));
				
				ReplicationService service = new ReplicationService(key, null, "UNREGISTER");
				service.start();
			} else if (request.getRequestType().equalsIgnoreCase("DOWNLOAD")) {
				String fileName = (String) request.getRequestData();
				log.write("Uploding/Sending file " + fileName);
				
				File file = new File(filesLocation + fileName);
				byte[] fileBytes = Files.readAllBytes(file.toPath());
				out.writeObject(fileBytes);
				out.flush();
				log.write("File sent successfully.");
			} else if (request.getRequestType().equalsIgnoreCase("R_DOWNLOAD")) {
				String fileName = (String) request.getRequestData();
				log.write("Uploding/Sending file " + fileName + " from Replica.");
				
				File file = new File(replicaLocation + fileName);
				byte[] fileBytes = Files.readAllBytes(file.toPath());
				out.writeObject(fileBytes);
				out.flush();
				log.write("File sent successfully.");
			} else if (request.getRequestType().equalsIgnoreCase("R_REGISTER")) {
				String data = (String) request.getRequestData();
				String key = data.split(",")[0];
				String value = data.split(",")[1];
		
				//System.out.println("\nR_REGISTER replicatedHashTable = " + DistributedHashTable.getReplicatedHashTable());
				log.write(String.format("Serving REPLICATE - REGISTER(%s,%s) request of %s.", key, value, clientIp));
				FileTransferSystem.putInReplicaHashTable(clientIp, key, value);
				
				//System.out.println("\nR_REGISTER replicatedHashTable = " + DistributedHashTable.getReplicatedHashTable());
				response = new Response();
				response.setResponseCode(200);
				response.setResponseData("(Key,Value) pair added successfully.");
				out.writeObject(response);
				
				//System.out.println(data + " " + clientIp);
				FileUtility.replicateFile(value, portAddress, key);
				
				log.write(String.format("REPLICATE - REGISTER(%s,%s) for %s completed successfully.", key, value, clientIp));
			} else if (request.getRequestType().equalsIgnoreCase("R_LOOKUP")) {
				String key = (String) request.getRequestData();
				
				log.write(String.format("Serving REPLICATE - LOOKUP(%s) request of %s.", key, clientIp));
				String value = null;
				
				value = FileTransferSystem.getFromReplicaHashTable(key);
				
				if (value != null) {
					response = new Response();
					response.setResponseCode(200);
					response.setResponseData(key + "," + value);
					out.writeObject(response);
					log.write(String.format("REPLICATE - LOOKUP(%s) = %s for %s completed successfully.", key, value, clientIp));
				} else {
					response = new Response();
					response.setResponseCode(404);
					response.setResponseData("VALUE with this KEY does not exist.");
					out.writeObject(response);
					log.write(String.format("REPLICATE - LOOKUP(%s) = %s for %s completed successfully. Key not found.", key, value, clientIp));
				}
			} else if (request.getRequestType().equalsIgnoreCase("R_UNREGISTER")) {					
				String key = (String) request.getRequestData();
				
				log.write(String.format("Serving REPLICATE - UNREGISTER(%s) request of %s.", key, clientIp));
				
				FileTransferSystem.removeFromReplicaHashTable(clientIp, key);

				response = new Response();
				response.setResponseCode(200);
				out.writeObject(response);
				
				if (FileUtility.deleteFile(key)) {
					log.write(String.format("File %s deleted.", key));
				} else {
					log.write(String.format("File %s could not be deleted.", key));
				}
				
				log.write(String.format("REPLICATE - UNREGISTER(%s) for %s completed successfully.", key, clientIp));
			} else if (request.getRequestType().equalsIgnoreCase("GET_R_HASHTABLE")) {					
				log.write(String.format("Serving GET_R_HASHTABLE request of %s.", clientIp));
				
				//System.out.println(DistributedHashTable.getReplicatedHashTable());
				HashMap<String, String> innerMap = FileTransferSystem.getReplicatedHashTable().get(clientIp);
				
				if (innerMap != null) {
					response = new Response();
					response.setResponseCode(200);
					response.setResponseData(innerMap);
					out.writeObject(response);
				} else {
					response = new Response();
					response.setResponseCode(404);
					out.writeObject(response);
				}
				
				log.write(String.format("DATA of %s sent successfully. Request completed. " + innerMap, clientIp));
			} else if (request.getRequestType().equalsIgnoreCase("GET_HASHTABLE")) {					
				log.write(String.format("Serving GET_HASHTABLE request of %s.", clientIp));
				//System.out.println("Sending Replication Hash Table = " + DistributedHashTable.getReplicatedHashTable());
				
				response = new Response();
				response.setResponseCode(200);
				response.setResponseData(FileTransferSystem.getHashTable());
				out.writeObject(response);
				
				log.write(String.format("HASH TABLE sent to %s successfully. Request completed. " + FileTransferSystem.getReplicatedHashTable(), clientIp));
			} else if (request.getRequestType().equalsIgnoreCase("GET_REPLICA")) {					
				log.write(String.format("Serving GET_REPLICA request of %s.", clientIp));
				
				response = new Response();
				response.setResponseCode(200);
				response.setResponseData(FileTransferSystem.getReplicatedHashTable());
				out.writeObject(response);
				
				log.write(String.format("REPLCATION DATA sent to %s successfully. Request completed. " + FileTransferSystem.getReplicatedHashTable(), clientIp));
			}
		} catch (Exception e) {
			//log.write("ERROR:" + e);
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
		log.close();
		super.interrupt();
	}
}