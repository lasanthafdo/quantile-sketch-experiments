package Power;

import java.util.ArrayList;
import java.util.Arrays;

public class PowerConstants {
    public static final String POWER_KAFKA_TOPIC_PREFIX = "power-events";
    public static final ArrayList<String> HEADERS = new ArrayList<>(Arrays.asList(
            "Date",
            "Time",
            "Global_active_power",
            "Global_reactive_power",
            "Voltage",
            "Global_intensity",
            "Sub_metering_1",
            "Sub_metering_2",
            "Sub_metering_3"
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