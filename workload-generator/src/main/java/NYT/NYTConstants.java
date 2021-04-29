package NYT;

import java.util.ArrayList;
import java.util.Arrays;

public class NYTConstants {
    public static final String NYT_KAFKA_TOPIC_PREFIX = "nyt-events";
    public static final ArrayList<String> HEADERS = new ArrayList<>(Arrays.asList(
            "medallion",
            "hack_license",
            "pickup_datetime",
            "dropoff_datetime",
            "trip_time_in_secs",
            "trip_distance",
            "pickup_longitude",
            "pickup_latitude",
            "dropoff_longitude",
            "dropoff_latitude",
            "payment_type",
            "fare_amount",
            "surcharge",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "total_amount"
            ));
}

/*
medallion
hack_lice
pickup_da
dropoff_d
trip_time
trip_dist
pickup_lo
pickup_la
dropoff_l
dropoff_l
payment_t
fare_amou
surcharge
mta_tax	t
tip_amoun
tolls_amo
total_amo
 */