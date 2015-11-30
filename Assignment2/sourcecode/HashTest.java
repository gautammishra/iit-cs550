import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HashTest {
	
	private static HashMap<Integer, Integer> bucket = new HashMap<Integer, Integer>();
	
	public static void main(String[] args) {
		// Testing Hash Function
		testHash("rawtext.txt", 10);
	}
	
	/***
	 * This method performs a hash on the key and returns the node id where the key is located.
	 * We get the hashCode of the input KEY string which is a number and then we perform MOD operation on that hashCode.
	 * The hashCode for a given string will always be same for that string.
	 * @param key	Key on which hash to be performed i.e. whose node has to be found.
	 * @return		Returns a node id (integer) where the key is located.
	 */
	private static int hash(String key, int networkSize) {
		// Total number of servers = Total number of entries in networkMap
		int totalServers = networkSize;
		
		// Get hash value of string using JAVA's hashCode() method
		long hashCode = key.hashCode();
		
		// hashCode may be negative. So, make it positive in case it's negative
		hashCode = (hashCode < 0) ? -hashCode : hashCode;
		
		// Add 1 because MOD of hashCode and totalServers may be 0
		int hash = (int) (hashCode % totalServers) + 1;
		return hash;
	}
	
	private static void testHash(String fileName , int networkSize) {
		BufferedReader br = null;
		ArrayList<String> wordList = new ArrayList<String>();
		
		try {
			for (int i = 1; i <= networkSize; i++) {
				bucket.put(i, 0);
			}
			
			long startTime = System.currentTimeMillis();
			
			br = new BufferedReader(new FileReader(fileName));
			String line = null;
			int count = 0, length = 0;
			
			while((line = br.readLine()) != null) {
				String[] words = line.split(" ");
				for (String word: words) {
					if (!wordList.contains(word)) {
						wordList.add(word);
						int hash = hash(word, networkSize);
						bucket.put(hash, bucket.get(hash) + 1);
						count++;
						length += word.length();
					}
				}
			}
			
			long endTime = System.currentTimeMillis();
			double time = (endTime - startTime) / 1000.0;
			
			System.out.println("UNIQUE WORDS COUNT: " + count);
			System.out.println("AVERAGE WORD LENGTH: " + length / count);
			System.out.println("**** BUCKET ****");
			
			for (Map.Entry e : bucket.entrySet()) {
				System.out.println(String.format("%s - %s", e.getKey(), e.getValue()));
			}
			
			System.out.println("Time to process: " + time + " seconds");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(br != null) 
					br.close();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}

	}
}
