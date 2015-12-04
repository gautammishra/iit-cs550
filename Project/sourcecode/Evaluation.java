import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import redis.clients.jedis.HostAndPort;

public class Evaluation {

	private static String localhost = NetworkUtility.getLocalAddress();
	private static final int KEY_SIZE = 10;
	private static final int VALUE_SIZE = 90;

	public static void main(String[] args) {
		String host = (args.length > 0 && args[0] != null) ? args[0] : "172.31.15.38";
		String system = (args.length > 0 && args[2] != null) ? args[2] : "all";
		int port = (args.length > 0 && args[1] != null) ? Integer.parseInt(args[1]) : 27020;
		int numOperations = (args.length > 0 && args[3] != null) ? Integer.parseInt(args[3]) : 10000;
		
		if (system.equalsIgnoreCase("mongodb")) {
			evaluateMongoDB(host, port, numOperations);
		} else if (system.equalsIgnoreCase("cassandra")) {
			evaluateCassandra(localhost, numOperations);
		} else if (system.equalsIgnoreCase("redis")) {
			evaluateRedis(host, port, numOperations);
		} else if (system.equalsIgnoreCase("mydht")) {
			evaluateMyDHT(host, numOperations);
		} else if (system.equalsIgnoreCase("riak")) {
			evaluateRiak(host, port, numOperations);
		}
	}

	private static void evaluateMongoDB(String host, int port, int numOperations) {

		System.out.println("Evaluating MongoDB.");
		int id = Integer.parseInt(localhost.substring(localhost.lastIndexOf(".") + 1, localhost.length())) + new Random().nextInt(50);

		long startKey = id * numOperations;
		long endKey = (id * numOperations) + numOperations - 1;

		long startTime, endTime, totalTime = 0;
		double time, throughput;

		// Connecting to Host i.e. MongoDB Query Router (Server)
		MongoDB.connect(host, port);

		System.out.println("Connected to MongoDB Query Router.");

		// Evaluating INSERT operation
		System.out.println("Evaluating MongoDB's INSERT Operation.");
		for (long i = startKey; i <= endKey; i++) {
			String key = padString(Long.toString(i), KEY_SIZE);
			String value = padString(Long.toString(i), VALUE_SIZE);

			startTime = System.currentTimeMillis();
			MongoDB.insert(key, value);
			endTime = System.currentTimeMillis();
			totalTime += (endTime - startTime);
		}

		time = totalTime / 1000.0;
		throughput = numOperations / time;

		System.out.printf("\nMongoDB INSERT operation test results:");
		System.out.printf("\nTotal time to execute %d INSERT operations = %f seconds", numOperations, time);
		System.out.printf("\nLATENCY - Average time per INSERT operation = %f milliseconds", totalTime / (double) numOperations);
		System.out.printf("\nTHROUGHPUT - Number of INSERT operations per second = %f", throughput);

		// Evaluating LOOKUP operation
		System.out.println("\n\nEvaluating MongoDB's LOOKUP Operation.");
		for (long i = startKey; i <= endKey; i++) {
			String key = padString(Long.toString(i), KEY_SIZE);

			startTime = System.currentTimeMillis();
			MongoDB.lookup(key);
			endTime = System.currentTimeMillis();
			totalTime += (endTime - startTime);
		}

		time = totalTime / 1000.0;
		throughput = numOperations / time;

		System.out.printf("\nMongoDB LOOKUP operation test results:");
		System.out.printf("\nTotal time to execute %d LOOKUP operations = %f seconds", numOperations, time);
		System.out.printf("\nLATENCY - Average time per LOOKUP operation = %f milliseconds", totalTime / (double) numOperations);
		System.out.printf("\nTHROUGHPUT - Number of LOOKUP operations per second = %f", throughput);

		// Evaluating REMOVE operation
		System.out.println("\n\nEvaluating MongoDB's REMOVE Operation.");
		for (long i = startKey; i <= endKey; i++) {
			String key = padString(Long.toString(i), KEY_SIZE);

			startTime = System.currentTimeMillis();
			MongoDB.remove(key);
			endTime = System.currentTimeMillis();
			totalTime += (endTime - startTime);
		}

		time = totalTime / 1000.0;
		throughput = numOperations / time;

		System.out.printf("\nMongoDB REMOVE operation test results:");
		System.out.printf("\nTotal time to execute %d REMOVE operations = %f seconds", numOperations, time);
		System.out.printf("\nLATENCY - Average time per REMOVE operation = %f milliseconds", totalTime / (double) numOperations);
		System.out.printf("\nTHROUGHPUT - Number of REMOVE operations per second = %f", throughput);
		System.out.println("\n");
	}

	private static void evaluateCassandra(String host, int numOperations) {

		System.out.println("Evaluating Cassandra.");
		int id = Integer.parseInt(localhost.substring(localhost.lastIndexOf(".") + 1, localhost.length())) + new Random().nextInt(50);

		long startKey = id * numOperations;
		long endKey = (id * numOperations) + numOperations - 1;

		long startTime, endTime, totalTime = 0;
		double time, throughput;

		// Connecting to Cassandra Cluster
		Cassandra.connect(host);

		System.out.println("Connected to Cassandra Cluster.");

		// Evaluating INSERT operation
		System.out.println("Evaluating Cassandra's INSERT Operation.");
		for (long i = startKey; i <= endKey; i++) {
			String key = padString(Long.toString(i), KEY_SIZE);
			String value = padString(Long.toString(i), VALUE_SIZE);

			startTime = System.currentTimeMillis();
			Cassandra.insert(key, value);
			endTime = System.currentTimeMillis();
			totalTime += (endTime - startTime);
		}

		time = totalTime / 1000.0;
		throughput = numOperations / time;

		System.out.printf("\nCassandra INSERT operation test results:");
		System.out.printf("\nTotal time to execute %d INSERT operations = %f seconds", numOperations, time);
		System.out.printf("\nLATENCY - Average time per INSERT operation = %f milliseconds", totalTime / (double) numOperations);
		System.out.printf("\nTHROUGHPUT - Number of INSERT operations per second = %f", throughput);

		// Evaluating LOOKUP operation
		System.out.println("\n\nEvaluating Cassandra's LOOKUP Operation.");
		for (long i = startKey; i <= endKey; i++) {
			String key = padString(Long.toString(i), KEY_SIZE);

			startTime = System.currentTimeMillis();
			Cassandra.lookup(key);
			endTime = System.currentTimeMillis();
			totalTime += (endTime - startTime);
		}

		time = totalTime / 1000.0;
		throughput = numOperations / time;

		System.out.printf("\nCassandra LOOKUP operation test results:");
		System.out.printf("\nTotal time to execute %d LOOKUP operations = %f seconds", numOperations, time);
		System.out.printf("\nLATENCY - Average time per LOOKUP operation = %f milliseconds", totalTime / (double) numOperations);
		System.out.printf("\nTHROUGHPUT - Number of LOOKUP operations per second = %f", throughput);

		// Evaluating REMOVE operation
		System.out.println("\n\nEvaluating Cassandra's REMOVE Operation.");
		for (long i = startKey; i <= endKey; i++) {
			String key = padString(Long.toString(i), KEY_SIZE);

			startTime = System.currentTimeMillis();
			Cassandra.remove(key);
			endTime = System.currentTimeMillis();
			totalTime += (endTime - startTime);
		}

		time = totalTime / 1000.0;
		throughput = numOperations / time;

		System.out.printf("\nCassandra REMOVE operation test results:");
		System.out.printf("\nTotal time to execute %d REMOVE operations = %f seconds", numOperations, time);
		System.out.printf("\nLATENCY - Average time per REMOVE operation = %f milliseconds", totalTime / (double) numOperations);
		System.out.printf("\nTHROUGHPUT - Number of REMOVE operations per second = %f", throughput);
		System.out.println("\n");

		Cassandra.disconnect();
	}
	
	private static void evaluateRedis(String host, int port, int numOperations) {

		System.out.println("Evaluating Redis.");
		int id = Integer.parseInt(localhost.substring(localhost.lastIndexOf(".") + 1, localhost.length())) + new Random().nextInt(50);

		long startKey = id * numOperations;
		long endKey = (id * numOperations) + numOperations - 1;

		long startTime, endTime, totalTime = 0;
		double time, throughput;

		// Connecting to Redis Cluster
		if (NetworkUtility.validate(host)) {
			Redis.connect(host, port);
		} else {
			Redis.connect(getClusterNodes(host));
		}

		System.out.println("Connected to Redis Cluster.");

		// Evaluating INSERT operation
		System.out.println("Evaluating Redis INSERT Operation.");
		for (long i = startKey; i <= endKey; i++) {
			String key = padString(Long.toString(i), KEY_SIZE);
			String value = padString(Long.toString(i), VALUE_SIZE);

			startTime = System.currentTimeMillis();
			Redis.insert(key, value);
			endTime = System.currentTimeMillis();
			totalTime += (endTime - startTime);
		}

		time = totalTime / 1000.0;
		throughput = numOperations / time;

		System.out.printf("\nRedis INSERT operation test results:");
		System.out.printf("\nTotal time to execute %d INSERT operations = %f seconds", numOperations, time);
		System.out.printf("\nLATENCY - Average time per INSERT operation = %f milliseconds", totalTime / (double) numOperations);
		System.out.printf("\nTHROUGHPUT - Number of INSERT operations per second = %f", throughput);

		// Evaluating LOOKUP operation
		System.out.println("\n\nEvaluating Redis LOOKUP Operation.");
		for (long i = startKey; i <= endKey; i++) {
			String key = padString(Long.toString(i), KEY_SIZE);

			startTime = System.currentTimeMillis();
			Redis.lookup(key);
			endTime = System.currentTimeMillis();
			totalTime += (endTime - startTime);
		}

		time = totalTime / 1000.0;
		throughput = numOperations / time;

		System.out.printf("\nRedis LOOKUP operation test results:");
		System.out.printf("\nTotal time to execute %d LOOKUP operations = %f seconds", numOperations, time);
		System.out.printf("\nLATENCY - Average time per LOOKUP operation = %f milliseconds", totalTime / (double) numOperations);
		System.out.printf("\nTHROUGHPUT - Number of LOOKUP operations per second = %f", throughput);

		// Evaluating REMOVE operation
		System.out.println("\n\nEvaluating Redis REMOVE Operation.");
		for (long i = startKey; i <= endKey; i++) {
			String key = padString(Long.toString(i), KEY_SIZE);

			startTime = System.currentTimeMillis();
			Redis.remove(key);
			endTime = System.currentTimeMillis();
			totalTime += (endTime - startTime);
		}

		time = totalTime / 1000.0;
		throughput = numOperations / time;

		System.out.printf("\nRedis REMOVE operation test results:");
		System.out.printf("\nTotal time to execute %d REMOVE operations = %f seconds", numOperations, time);
		System.out.printf("\nLATENCY - Average time per REMOVE operation = %f milliseconds", totalTime / (double) numOperations);
		System.out.printf("\nTHROUGHPUT - Number of REMOVE operations per second = %f", throughput);
		System.out.println("\n");
		
		Redis.disconnect();
	}

	private static void evaluateMyDHT(String host, int numOperations) {

		System.out.println("Evaluating MyDHT.");
		int id = Integer.parseInt(localhost.substring(localhost.lastIndexOf(".") + 1, localhost.length())) + new Random().nextInt(50);

		long startKey = id * numOperations;
		long endKey = (id * numOperations) + numOperations - 1;

		long startTime, endTime, totalTime = 0;
		double time, throughput;

		// Connecting to MyDHT (Server)
		MyDHT.connect(host);

		System.out.println("Connected to MyDHT Query Router.");

		// Evaluating INSERT operation
		System.out.println("Evaluating MyDHT's INSERT Operation.");
		for (long i = startKey; i <= endKey; i++) {
			String key = padString(Long.toString(i), KEY_SIZE);
			String value = padString(Long.toString(i), VALUE_SIZE);

			startTime = System.currentTimeMillis();
			MyDHT.insert(key, value);
			endTime = System.currentTimeMillis();
			totalTime += (endTime - startTime);
		}

		time = totalTime / 1000.0;
		throughput = numOperations / time;

		System.out.printf("\nMyDHT INSERT operation test results:");
		System.out.printf("\nTotal time to execute %d INSERT operations = %f seconds", numOperations, time);
		System.out.printf("\nLATENCY - Average time per INSERT operation = %f milliseconds", totalTime / (double) numOperations);
		System.out.printf("\nTHROUGHPUT - Number of INSERT operations per second = %f", throughput);

		// Evaluating LOOKUP operation
		System.out.println("\n\nEvaluating MyDHT's LOOKUP Operation.");
		for (long i = startKey; i <= endKey; i++) {
			String key = padString(Long.toString(i), KEY_SIZE);

			startTime = System.currentTimeMillis();
			MyDHT.lookup(key);
			endTime = System.currentTimeMillis();
			totalTime += (endTime - startTime);
		}

		time = totalTime / 1000.0;
		throughput = numOperations / time;

		System.out.printf("\nMyDHT LOOKUP operation test results:");
		System.out.printf("\nTotal time to execute %d LOOKUP operations = %f seconds", numOperations, time);
		System.out.printf("\nLATENCY - Average time per LOOKUP operation = %f milliseconds", totalTime / (double) numOperations);
		System.out.printf("\nTHROUGHPUT - Number of LOOKUP operations per second = %f", throughput);

		// Evaluating REMOVE operation
		System.out.println("\n\nEvaluating MyDHT's REMOVE Operation.");
		for (long i = startKey; i <= endKey; i++) {
			String key = padString(Long.toString(i), KEY_SIZE);

			startTime = System.currentTimeMillis();
			MyDHT.remove(key);
			endTime = System.currentTimeMillis();
			totalTime += (endTime - startTime);
		}

		time = totalTime / 1000.0;
		throughput = numOperations / time;

		System.out.printf("\nMyDHT REMOVE operation test results:");
		System.out.printf("\nTotal time to execute %d REMOVE operations = %f seconds", numOperations, time);
		System.out.printf("\nLATENCY - Average time per REMOVE operation = %f milliseconds", totalTime / (double) numOperations);
		System.out.printf("\nTHROUGHPUT - Number of REMOVE operations per second = %f", throughput);
		System.out.println("\n");
	}
	
	private static void evaluateRiak(String host, int port, int numOperations) {

		System.out.println("Evaluating Riak.");
		int id = Integer.parseInt(localhost.substring(localhost.lastIndexOf(".") + 1, localhost.length())) + new Random().nextInt(50);

		long startKey = id * numOperations;
		long endKey = (id * numOperations) + numOperations - 1;

		long startTime, endTime, totalTime = 0;
		double time, throughput;

		// Connecting to Riak (Server)
		Riak.connect(host, port);

		System.out.println("Connected to Riak Query Router.");

		// Evaluating INSERT operation
		System.out.println("Evaluating Riak's INSERT Operation.");
		for (long i = startKey; i <= endKey; i++) {
			String key = padString(Long.toString(i), KEY_SIZE);
			String value = padString(Long.toString(i), VALUE_SIZE);

			startTime = System.currentTimeMillis();
			Riak.insert(key, value);
			endTime = System.currentTimeMillis();
			totalTime += (endTime - startTime);
		}

		time = totalTime / 1000.0;
		throughput = numOperations / time;

		System.out.printf("\nRiak INSERT operation test results:");
		System.out.printf("\nTotal time to execute %d INSERT operations = %f seconds", numOperations, time);
		System.out.printf("\nLATENCY - Average time per INSERT operation = %f milliseconds", totalTime / (double) numOperations);
		System.out.printf("\nTHROUGHPUT - Number of INSERT operations per second = %f", throughput);

		// Evaluating LOOKUP operation
		System.out.println("\n\nEvaluating Riak's LOOKUP Operation.");
		for (long i = startKey; i <= endKey; i++) {
			String key = padString(Long.toString(i), KEY_SIZE);

			startTime = System.currentTimeMillis();
			Riak.lookup(key);
			endTime = System.currentTimeMillis();
			totalTime += (endTime - startTime);
		}

		time = totalTime / 1000.0;
		throughput = numOperations / time;

		System.out.printf("\nRiak LOOKUP operation test results:");
		System.out.printf("\nTotal time to execute %d LOOKUP operations = %f seconds", numOperations, time);
		System.out.printf("\nLATENCY - Average time per LOOKUP operation = %f milliseconds", totalTime / (double) numOperations);
		System.out.printf("\nTHROUGHPUT - Number of LOOKUP operations per second = %f", throughput);

		// Evaluating REMOVE operation
		System.out.println("\n\nEvaluating Riak's REMOVE Operation.");
		for (long i = startKey; i <= endKey; i++) {
			String key = padString(Long.toString(i), KEY_SIZE);

			startTime = System.currentTimeMillis();
			Riak.remove(key);
			endTime = System.currentTimeMillis();
			totalTime += (endTime - startTime);
		}

		time = totalTime / 1000.0;
		throughput = numOperations / time;

		System.out.printf("\nRiak REMOVE operation test results:");
		System.out.printf("\nTotal time to execute %d REMOVE operations = %f seconds", numOperations, time);
		System.out.printf("\nLATENCY - Average time per REMOVE operation = %f milliseconds", totalTime / (double) numOperations);
		System.out.printf("\nTHROUGHPUT - Number of REMOVE operations per second = %f", throughput);
		System.out.println("\n");
		
		Riak.disconnect();
	}
	
	private static String padString(String string, int length) {
		StringBuffer paddedString = new StringBuffer();
		paddedString.append(string);

		for (int i = 1; i <= length - (string.length()); i++) {
			paddedString.append("#");
		}
		return paddedString.toString();
	}
	
	private static Set<HostAndPort> getClusterNodes(String fileName) {
		File file = new File(fileName);
		if (file.exists()) {
			BufferedReader br = null;
			try {
				Set<HostAndPort> clusterNodes = new HashSet<HostAndPort>();
				br = new BufferedReader(new FileReader(fileName));
				String line = null;
				
				System.out.println("\nCluster Details:");
				while ((line = br.readLine()) != null) {
					clusterNodes.add(new HostAndPort(line.split(" ")[0], Integer.parseInt(line.split(" ")[1])));
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
}
