package org.uwaterloo.streaming.benchmarks.workload.generator.nyt;

import java.util.ArrayList;
import java.util.Arrays;

public class NYTConstants {
    public static final String NYT_KAFKA_TOPIC_PREFIX = "nyt-events";
    public static final ArrayList<String> HEADERS = new ArrayList<>(Arrays.asList(
        "medallion", // 0
        "hack_license", // 1
        "vendor_id", //2
        "pickup_datetime", // 3
        "payment_type", // 4
        "fare_amount", // 5
        "surcharge", // 6
        "mta_tax", // 7
        "tip_amount", // 8
        "tolls_amount", // 9
        "total_amount" // 10
    ));
}
