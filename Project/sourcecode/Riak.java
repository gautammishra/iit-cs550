import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

public class Riak {

	private static RiakCluster cluster = null;
	private static RiakClient client = null;
	private static Namespace bucket = null;

	private static List<RiakNode> getClusterNodes(String fileName, int port) {
		File file = new File(fileName);
		if (file.exists()) {
			BufferedReader br = null;
			try {
				LinkedList<RiakNode> clusterNodes = new LinkedList<RiakNode>();
				br = new BufferedReader(new FileReader(fileName));
				String line = null;
				
				RiakNode.Builder node = new RiakNode.Builder().withMinConnections(10).withMaxConnections(50);
				
				System.out.println("\nCluster Details:");
				while ((line = br.readLine()) != null) {
					String host = line.split(" ")[0].trim();

					clusterNodes.add(node.withRemoteAddress(host).withRemotePort(port).build());
					System.out.println(line);
				}
				return clusterNodes;
			} catch (Exception e) {
				System.out.println("Error Reading File:" + e);
			} finally {
				try {
					if (br != null)
						br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} else {
			System.out.println("Unable to read cluster nodes file.");
		}
		return null;
	}

	public static void connect(String fileName, int port) {
		// And now we can use our setUpCluster() function to create a cluster
		// object which we can then use to create a client object and then
		// execute our storage operation
		try {
			// This cluster object takes our one node as an argument
			cluster = new RiakCluster.Builder(getClusterNodes(fileName, port)).build();

			// The cluster must be started to work, otherwise you will see errors
			cluster.start();
			client = new RiakClient(cluster);
			
			/*LinkedList<String> ipAddresses = new LinkedList<String>();
	        ipAddresses.add("172.31.12.200");
	        ipAddresses.add("172.31.1.228");
			client = RiakClient.newClient(8087, ipAddresses);*/

			// In the new Java client, instead of buckets you interact with Namespace
			// objects, which consist of a bucket AND a bucket type; if you don't
			// supply a bucket type, "default" is used; the Namespace below will set
			// only a bucket, without supplying a bucket type
			bucket = new Namespace("hashtable");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	public static boolean insert(String key, String value) {
		// First, we'll create a basic object storing a movie quote
		RiakObject quoteObject = new RiakObject()
				// We tell Riak that we're storing plaintext, not JSON, HTML,
				// etc.
				.setContentType("text/plain")
				// Objects are ultimately stored as binaries
				.setValue(BinaryValue.create(value));

		// With our Namespace object in hand, we can create a Location object,
		// which allows us to pass in a key as well
		Location quoteObjectLocation = new Location(bucket, key);

		// With our RiakObject in hand, we can create a StoreValue operation
		StoreValue storeOp = new StoreValue.Builder(quoteObject).withLocation(quoteObjectLocation).build();

		try {
			client.execute(storeOp);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}

	public static String lookup(String key) {
		Location quoteObjectLocation = new Location(bucket, key);
		// Now we can verify that the object has been stored properly by
		// creating and executing a FetchValue operation
		FetchValue fetchOp = new FetchValue.Builder(quoteObjectLocation).build();
		RiakObject fetchedObject = null;
		try {
			fetchedObject = client.execute(fetchOp).getValue(RiakObject.class);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		if (fetchedObject != null) {
			return fetchedObject.getValue().toString();
		}
		return null;
	}

	public static boolean remove(String key) {
		Location quoteObjectLocation = new Location(bucket, key);
		DeleteValue deleteOp = new DeleteValue.Builder(quoteObjectLocation).build();

		try {
			client.execute(deleteOp);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static void disconnect() {
		//cluster.shutdown();
		client.shutdown();
	}

	/*public static void main(String[] args) {
		//connect("52.10.166.58", 8087);
		connect("C:/Users/Gautam/Eclipse Workspace/CS550_Assignment4/src/abc.txt", 8087);
		System.out.println("connected");
		for (int i = 0; i < 1000; i++) {
			//insert(i + "", i + "");
			//System.out.println(lookup("gautam"));
			remove(i + "");
		}
		disconnect();
		System.out.println("disconnected");
	}*/

}
