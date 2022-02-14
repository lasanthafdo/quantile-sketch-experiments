package NYT;

import java.util.ArrayList;
import java.util.Arrays;

public class NYTConstants {
    public static final String NYT_KAFKA_TOPIC_PREFIX = "nyt-events";
    public static final ArrayList<String> HEADERS = new ArrayList<>(Arrays.asList(
            "medallion", // 0
            "hack_license", // 1
            "pickup_datetime", // 2
            "dropoff_datetime", // 3
            "trip_time_in_secs", // 4
            "trip_distance", // 5
            "pickup_longitude", // 6
            "pickup_latitude", // 7
            "dropoff_longitude", // 8
            "dropoff_latitude", // 9
            "payment_type", // 10
            "fare_amount", // 11
            "surcharge", // 12
            "mta_tax", // 13
            "tip_amount", // 14
            "tolls_amount", // 15
            "total_amount" // 16
            ));
}
