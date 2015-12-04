import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Cassandra {

	private static Cluster cluster;
	private static Session session;
	
	public static void connect(String host) {
		// Connect to the cluster and keyspace "hashtable"
		cluster = Cluster.builder().addContactPoint(host).build();
		session = cluster.connect("cs550");
	}
	
	public static boolean insert(String key, String value) {
		// Insert one record into the table
		String query = String.format("INSERT INTO hashtable (key, value) VALUES ('%s', '%s')", key, value);
		session.execute(query);
		return true;
	}
	
	public static boolean remove(String key) {
		// Delete one record into the table
		String query = String.format("DELETE FROM hashtable WHERE key = '%s'", key);
		session.execute(query);
		return true;
	}
	
	public static String lookup(String key) {
		// Delete one record into the table
		String query = String.format("SELECT value FROM hashtable WHERE key = '%s'", key);
		ResultSet results = session.execute(query);
		for (Row row : results) {
			return row.getString("value");
		}
		return null;
	}
	
	public static void disconnect() {
		// Clean up the connection by closing it
		cluster.close();
	}
	
	public static void main(String[] args) {
		
		connect("52.34.230.108");
		//connect("127.0.0.1");
		for (int i = 1; i <= 500; i++) {
			//insert(i + "", i + "");
			//System.out.print(i + " ");
			//System.out.println(lookup(i + ""));
			remove(i + "");
		}
		disconnect();
	}

}
