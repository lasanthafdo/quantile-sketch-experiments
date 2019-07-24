package YSB;

import java.util.ArrayList;
import java.util.List;

import static java.util.UUID.randomUUID;

public class YSBUtils {

    static List<String> generateIds(int n) {
        ArrayList<String> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(randomUUID().toString());
        }
        return list;
    }
}
