package NYT;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class TaxiQuery implements Runnable {

    /* The Kafka topic the source operators are pulling the results from */
    private final String KAFKA_PREFIX_TOPIC = "nyt-events";
    /* The Job Parameters */
    //This class provides simple utility methods for reading and parsing program
    // arguments from different sources.
    // Only single value parameter could be supported in args.
    private final ParameterTool setupParams;
    /* The scheduler policy */
//    private final StreamTaskSchedulerPolicy schedulerPolicy;
    /* The num of queries */
    private final int numQueries;
    /* The window size */
    private final int windowSize;
    private JSONObject js;

    public TaxiQuery(
            ParameterTool setupParams, int numQueries, int windowSize) {
        this.setupParams = setupParams;
//        this.schedulerPolicy = schedulerPolicy;
        this.numQueries = numQueries;
        this.windowSize = windowSize;
    }


    @Override
    public void run() {
        // Setup Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setStreamTaskSchedulerPolicy(schedulerPolicy);
        env.getConfig().setGlobalJobParameters(setupParams);

        // Add queries
        addQuery(env);

        // Execute queries
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void addQuery(StreamExecutionEnvironment env) {

        WatermarkStrategy<String> wt = WatermarkStrategy.<String>noWatermarks()
                .withTimestampAssigner((event, timestamp) -> Long.parseLong(new JSONObject(event).getString("event_time")));
        //Long.parseLong(new JSONObject(event).getString("event_time")
        DataStream<String> messageStream = env.addSource(
                // Every source is a Kafka Consumer
                new FlinkKafkaConsumer<>(
                        // Different topics for different query queryInstances
                        KAFKA_PREFIX_TOPIC,
                        new SimpleStringSchema(),
                        setupParams.getProperties()).assignTimestampsAndWatermarks(wt).setStartFromEarliest())
                .name("Source").setParallelism(1);
                // Chain all operators before a watermark is emitted.
                //.disableChaining();

        messageStream
                // Parse the JSON string from Kafka as an ad
                .map(new DeserializeAdsFromkafka())
                .name("DeserializeInput ")
                .disableChaining()
                .<Tuple6<String, String, String, String, String, String>>project(11, 2, 3, 4, 5, 17)
                // 11 - fare_amount
                // 2 - pickup_datetime
                // 3 - dropoff_datetime
                .name("project ")
                //.keyBy(0)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .aggregate(new WindowAdsAggregator())
                .name("Window")
                .writeAsText("results_nyt.txt", FileSystem.WriteMode.OVERWRITE);
                // sink function
                //.addSink(new PrintCampaignAdClicks())
                //.name("Sink(" + queryInstance + ")");
    }


    private class DeserializeAdsFromkafka implements
            MapFunction<String,
                        Tuple20<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, Boolean>> {

        @Override
        public Tuple20<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, Boolean> map(String input) {
            JSONObject obj = new JSONObject(input);
            Boolean fake = false;
            if (obj.has("fake")){
                fake = true;
            }
            return new Tuple20<>(
                    obj.getString("medallion"), // 0
                    obj.getString("hack_license"), // 1
                    obj.getString("pickup_datetime"), // 2
                    obj.getString("dropoff_datetime"), // 3
                    obj.getString("trip_time_in_secs"), // 4
                    obj.getString("trip_distance"), // 5
                    obj.getString("pickup_longitude"), // 6
                    obj.getString("pickup_latitude"), // 7
                    obj.getString("dropoff_longitude"), // 8
                    obj.getString("dropoff_latitude"), // 9
                    obj.getString("payment_type"), // 10
                    obj.getString("fare_amount"), // 11
                    obj.getString("surcharge"), // 12
                    obj.getString("mta_tax"), // 13
                    obj.getString("tip_amount"), // 14
                    obj.getString("tolls_amount"), // 15
                    obj.getString("total_amount"), // 16
                    obj.getString("event_time"), // 17
                    String.valueOf(System.currentTimeMillis()), // 18 ingestion_time
                    fake); // 19

        }
    }

    private class WindowAdsAggregator implements AggregateFunction<Tuple6<String, String, String, String, String, String>, Tuple3<Long, Double, Long>, Tuple3<Long, Double, Long>> {

        @Override
        public Tuple3<Long, Double, Long> createAccumulator() {
            return new Tuple3<>((long)1, (double)0, (long)0);
        }

        @Override
        public Tuple3<Long, Double, Long> add(Tuple6<String, String, String, String, String, String> value, Tuple3<Long, Double, Long> accumulator) {
            //windowSize
            int WINDOW_SIZE = 3000; // in milliseconds
            accumulator.f0 = Long.parseLong(value.f5)/WINDOW_SIZE;
            accumulator.f1 += Double.parseDouble(value.f0);
            accumulator.f2 += 1;
            return accumulator;
        }

        @Override
        public Tuple3<Long, Double, Long> merge(Tuple3<Long, Double, Long> acc0, Tuple3<Long, Double, Long> acc1) {
            assert acc0.f0.equals(acc1.f0);
            return new Tuple3<>(acc0.f0, acc0.f1 + acc1.f1, acc0.f2 + acc1.f2);
        }

        @Override
        public Tuple3<Long, Double, Long> getResult(Tuple3<Long, Double, Long> accumulator) {
            //return new Tuple3<>(accumulator.f0, accumulator.f1, accumulator.f2);
            Double d_output = round(accumulator.f1, 2);
            return new Tuple3<>(accumulator.f0, d_output, accumulator.f2);
        }

        private double round(double value, int places) {
            if (places < 0) throw new IllegalArgumentException();

            BigDecimal bd = new BigDecimal(Double.toString(value));
            bd = bd.setScale(places, RoundingMode.HALF_UP);
            return bd.doubleValue();
        }

    }
}
