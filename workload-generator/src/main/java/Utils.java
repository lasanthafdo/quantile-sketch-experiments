import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.UUID.randomUUID;

public class Utils {

    static List<String> generateIds(int n) {
        ArrayList<String> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(randomUUID().toString());
        }
        return list;
    }

    static Map yamlToMap(String fileName) throws IOException {
        InputStreamReader in = new InputStreamReader(new FileInputStream(new File(fileName)));
        Yaml yaml = new Yaml(new SafeConstructor());

        Map ret = yaml.load(in);
        if (ret == null) {
            return ret;
        }
        return new HashMap(ret);
    }

    static String getKafkaBrokers(Map conf) {
        if (!conf.containsKey("kafka.brokers")) {
            throw new IllegalArgumentException("No kafka brokers found!");
        }
        if (!conf.containsKey("kafka.port")) {
            throw new IllegalArgumentException("No kafka port found!");
        }
        return listOfStringToString((List<String>) conf.get("kafka.brokers"), String.valueOf(conf.get("kafka.port")));
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
}
