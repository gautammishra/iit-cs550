import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

public class Redis {

	private static Jedis redis = null;
	private static JedisCluster redisCluster = null;
	
	public static void connect(String host, int port) {
		try {
			redis = new Jedis(host, port);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Redis Connection Successful.");
	}
	
	public static void connect(Set<HostAndPort> clusterNodes) {
		try {
			redisCluster = new JedisCluster(clusterNodes);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Redis Connection Successful.");
	}

	public static boolean insert(String key, String value) {
		if (redis != null) {
			redis.set(key, value);
		} else {
			redisCluster.set(key, value);
		}
		return true;
	}

	public static boolean remove(String key) {
		if (redis != null) {
			redis.del(key);
		} else {
			redisCluster.del(key);
		}
		return true;
	}

	public static String lookup(String key) {
		if (redis != null) {
			return redis.get(key);
		} else {
			return redisCluster.get(key);
		}
	}
	
	public static void disconnect() {
		if (redis != null) {
			redis.close();
		} else {
			redisCluster.close();
		}
	}

	/*public static void main(String[] args) {

		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		//Jedis Cluster will attempt to discover cluster nodes automatically
		//52.34.4.128:7001 52.34.206.181:7002 52.34.51.137:7003 52.34.208.129:7004 52.34.14.6:7005 52.34.20.63:7006
		
		jedisClusterNodes.add(new HostAndPort("52.34.4.128", 7001));
		jedisClusterNodes.add(new HostAndPort("52.34.206.181", 7002));
		jedisClusterNodes.add(new HostAndPort("52.34.51.137", 7003));
		jedisClusterNodes.add(new HostAndPort("52.34.208.129", 7004));
		jedisClusterNodes.add(new HostAndPort("52.34.14.6", 7005));
		jedisClusterNodes.add(new HostAndPort("52.34.20.63", 7006));
		JedisCluster jc = new JedisCluster(jedisClusterNodes);
		//jc.
		//connect("52.34.206.181", 7002);

		for (int i = 0; i < 100000; i++) {
			jc.set(i + "", i + "#");
			//insert(i + "", i + "#");
			//System.out.print(i + " ");
			//System.out.println(lookup(i + ""));
			//remove(i + "");
			//jc.del(i + "");
		}
		
		//disconnect();
		jc.close();

	}*/

}
