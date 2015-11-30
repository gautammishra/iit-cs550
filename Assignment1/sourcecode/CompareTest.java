import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Random;

public class CompareTest {

	private final static int TEST_COUNT = 1000;
	
	public static void main(String[] args) {
		BufferedReader input = null;
        
		try {
			input = new BufferedReader(new InputStreamReader(System.in));
			String hostAddress, fileName;
			
			// Display different choices to the user
			System.out.println("\nWhat do you want to test?");
			System.out.println("1.Register");
			System.out.println("2.Lookup");
			System.out.println("3.Download");
			System.out.println("4.Exit.");
			System.out.print("Enter choice and press ENTER:");
			int option = 0;

			// Check if the user has entered only numbers.
			try {
				option = Integer.parseInt(input.readLine());
			} catch (NumberFormatException e) {
				System.out.println("Wrong choice. Try again!!!");
				System.exit(0);
			}
			
			switch (option) {
				case 1:
					System.out.println("\nEnter server address:");
					hostAddress = input.readLine();
					testRegister(hostAddress);
					break;
	
				case 2:
					System.out.println("\nEnter server address:");
					hostAddress = input.readLine();
					testLookup(hostAddress);
					break;
					
				case 3:
					System.out.println("\nEnter peer address and two file names you want to download:");
					hostAddress = input.readLine();
					fileName = input.readLine();
					testDownload(hostAddress, fileName);
					break;
					
				case 4:
					System.out.println("Thanks for using this system.");
					System.exit(0);
					break;
					
				default:
					System.out.println("Wrong choice. Try again!!!");
					break;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void testRegister(String serverAddress) {		
		Socket socket = null;
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		Request peerRequest = null;
		Response serverResponse	= null;
		long startTime, endTime, totalTime = 0;
		double avgTime;
		
		int clientId =  (new Random()).nextInt(50);
		long startKey = clientId * TEST_COUNT;
		long endKey = (clientId * TEST_COUNT) +  TEST_COUNT - 1;
		
		System.out.println("startKey = " + startKey);
		System.out.println("endKey = " + endKey);
		
		try {
			socket = new Socket(serverAddress, 10000);
	        out = new ObjectOutputStream(socket.getOutputStream());
	        out.flush();
	        in = new ObjectInputStream(socket.getInputStream());
	        startTime = System.currentTimeMillis();
	        
	        for (long i = startKey; i <= endKey; i++) {
				ArrayList<String> files = new ArrayList<String>();
				files.add(0, "files/" );
				files.add(1, Long.toString(i));
				
				peerRequest = new Request();
				peerRequest.setRequestType("REGISTER");
				peerRequest.setRequestData(files);
				out.writeObject(peerRequest);
				
				serverResponse = (Response) in.readObject();
			}
	        endTime = System.currentTimeMillis();
	        totalTime = endTime - startTime;
			avgTime = (double) Math.round(totalTime / (double) TEST_COUNT) / 1000;

			System.out.println("TOTAL TESTS : " + TEST_COUNT);
			System.out.println("TOTAL TIME : " + totalTime + " seconds");
			System.out.println("AVERAGE REGISTER TIME : " + avgTime + " seconds");
		} catch (Exception e) {
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
	}
	
	private static void testLookup(String serverAddress) {
		Socket socket = null;
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		Request peerRequest = null;
		Response serverResponse	= null;
		long startTime, endTime, totalTime = 0;
		double avgTime;
		
		int clientId =  (new Random()).nextInt(50);
		long startKey = clientId * TEST_COUNT;
		long endKey = (clientId * TEST_COUNT) +  TEST_COUNT - 1;
		
		System.out.println("startKey = " + startKey);
		System.out.println("endKey = " + endKey);
		
		try {
			socket = new Socket(serverAddress, 10000);
			BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
	        out = new ObjectOutputStream(socket.getOutputStream());
	        out.flush();
	        in = new ObjectInputStream(socket.getInputStream());
	        startTime = System.currentTimeMillis();
	        
	        for (long i = startKey; i <= endKey; i++) {
				peerRequest = new Request();
				peerRequest.setRequestType("LOOKUP");
				peerRequest.setRequestData(Long.toString(i));
				out.writeObject(peerRequest);
				
				serverResponse = (Response) in.readObject();
			}
	        endTime = System.currentTimeMillis();
	        totalTime = endTime - startTime;
			avgTime = (double) Math.round(totalTime / (double) TEST_COUNT) / 1000;

			System.out.println("TOTAL TESTS : " + TEST_COUNT);
			System.out.println("TOTAL TIME : " + totalTime + " seconds");
			System.out.println("AVERAGE SEARCH TIME : " + avgTime + " seconds");
		} catch (Exception e) {
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
	}
	
	private static void testDownload(String peerAddress, String fileName) {
		long startTime, endTime, totalTime = 0, totalFileSize = 0;
		double time, avgSpeed;
		System.out.println("Test Started...");
		
		try {
			for (int i = 0; i < TEST_COUNT; i++) {
				startTime = System.currentTimeMillis();
				FileUtility.downloadFile(peerAddress, 20000, fileName);
				endTime = System.currentTimeMillis();
				totalTime += (endTime - startTime);
				File file = new File("downloads/" + fileName);
				totalFileSize += file.length();
				file.delete();
			}
			time = (double) Math.round(totalTime / 1000.0);
			avgSpeed = (totalFileSize / (1024 * 1024)) / time;
			
			System.out.println("TOTAL TIME : " + time + " seconds");
			System.out.println("Average speed for downloading " + TEST_COUNT + " files is " + avgSpeed + " MBps.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
