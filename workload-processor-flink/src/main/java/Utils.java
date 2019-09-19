import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.*;
import java.net.URL;
import java.util.*;

class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    static Map<String, String> getFlinkConfs(Map conf) {
        String kafkaBrokers = getKafkaBrokers(conf);
        String zookeeperServers = getZookeeperServers(conf);

        Map<String, String> flinkConfs = new HashMap<>();
        flinkConfs.put("bootstrap.servers", kafkaBrokers);
        flinkConfs.put("zookeeper.connect", zookeeperServers);
        flinkConfs.put("jedis_server", getRedisHost(conf));
        flinkConfs.put("group.id", "myGroup");
        return flinkConfs;
    }

    static Map findAndReadConfigFile(String name) {
        InputStream in;
        try {
            in = getConfigFileInputStream(name);

            if (in == null) {
                throw new RuntimeException("Could not find config file on classpath " + name);
            }

            Yaml yaml = new Yaml(new SafeConstructor());
            Map ret = yaml.load(new InputStreamReader(in));
            if (ret != null) {
                return new HashMap(ret);
            } else {
                throw new RuntimeException("Config file " + name + " doesn't have any valid configs");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new HashMap();
    }

    private static String getZookeeperServers(Map conf) {
        if (!conf.containsKey("zookeeper.servers")) {
            throw new IllegalArgumentException("Not zookeeper servers found!");
        }
        return listOfStringToString((List<String>) conf.get("zookeeper.servers"), String.valueOf(conf.get("zookeeper.port")));
    }

    private static String getKafkaBrokers(Map conf) {
        if (!conf.containsKey("kafka.brokers")) {
            throw new IllegalArgumentException("No kafka brokers found!");
        }
        if (!conf.containsKey("kafka.port")) {
            throw new IllegalArgumentException("No kafka port found!");
        }
        String kafkaStr = (String) conf.get("kafka.brokers");
        int kafkaPort = (int) conf.get("kafka.port");
        String[] parts = kafkaStr.split(" ");
        String result = "";
        for(int i = 0; i < parts.length; i++) {
            result = result + " " + parts[i] + ":" + (kafkaPort + i);
            if (i < parts.length - 1) {
                result += ",";
            }
        }
        return result;
    }

    private static String getRedisHost(Map conf) {
        if (!conf.containsKey("redis.host")) {
            throw new IllegalArgumentException("No redis host found!");
        }
        return (String) conf.get("redis.host");
    }

    private static String listOfStringToString(List<String> list, String port) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            sb.append(list.get(i)).append(":").append(port);
            if (i < list.size() - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    private static InputStream getConfigFileInputStream(String configFilePath)
            throws IOException {
        if (null == configFilePath) {
            throw new IOException(
                    "Could not find config file, name not specified");
        }

        HashSet<URL> resources = new HashSet<>(findResources(configFilePath));
        if (resources.isEmpty()) {
            File configFile = new File(configFilePath);
            if (configFile.exists()) {
                return new FileInputStream(configFile);
            }
        } else if (resources.size() > 1) {
            throw new IOException(
                    "Found multiple " + configFilePath
                            + " resources. You're probably bundling the Storm jars with your topology jar. "
                            + resources);
        } else {
            LOG.debug("Using " + configFilePath + " from resources");
            URL resource = resources.iterator().next();
            return resource.openStream();
        }
        return null;
    }

    private static List<URL> findResources(String name) {
        try {
            Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(name);
            List<URL> ret = new ArrayList<>();
            while (resources.hasMoreElements()) {
                ret.add(resources.nextElement());
            }
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}

