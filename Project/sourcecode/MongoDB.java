import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

public class MongoDB {

	private static MongoClient mongo = null;
	private static DB db = null;
	private static DBCollection table = null;

	public static void connect(String host, int port) {
		try {

			/**** Connect to MongoDB ****/
			mongo = new MongoClient(host, port);

			/**** Get database ****/
			// if database doesn't exists, MongoDB will create it for you
			db = mongo.getDB("cs550");

			/**** Get collection / table from 'testdb' ****/
			// if collection doesn't exists, MongoDB will create it for you
			table = db.getCollection("hashtable");

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

	public static boolean insert(String key, String value) {
		try {
			// create a document to store key and value
			BasicDBObject query = new BasicDBObject();
			query.put("key", key);
			query.put("value", value);
			table.insert(query);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	public static boolean remove(String key) {
		try {
			BasicDBObject query = new BasicDBObject();
			query.put("key", key);
			table.remove(query);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public static String lookup(String key) {
		try {
			/**** Find and display ****/
			BasicDBObject query = new BasicDBObject();
			query.put("key", key);

			DBCursor cursor = table.find(query);

			while (cursor.hasNext()) {
				return cursor.next().toString();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/*public static void main(String[] args) {

		MongoDB.connect("52.34.230.149", 27020);

		for (int i = 0; i < 10; i++) {
			if (MongoDB.insert(i + "", i + "")) {
				System.out.println(i);
			}
		}
		
		for (int i = 0; i < 10; i++) {
			// System.out.println(MongoDB.lookup("" + i));
			MongoDB.remove(i + "");
		}
		System.out.println("ci");

		System.out.println(MongoDB.lookup("gautam"));
		MongoDB.remove("gautam");
		System.out.println(MongoDB.lookup("gautam"));
	}*/
}
