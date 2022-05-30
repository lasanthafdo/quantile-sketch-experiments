package org.uwaterloo.streaming.benchmarks.workload.processor.uddsketch;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import org.apache.flink.shaded.guava18.com.google.common.base.Preconditions;

import com.datadoghq.sketch.uddsketch.UniformDDSketch;
import org.apache.commons.math3.util.FastMath;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;

import static java.lang.Double.parseDouble;

public class TaxiQueryUDDSketch implements Runnable {

    /* The Kafka topic the source operators are pulling the results from */
    private final String KAFKA_PREFIX_TOPIC = "nyt-events";
    /* The Job Parameters */
    //This class provides simple utility methods for reading and parsing program
    // arguments from different sources.
    // Only single value parameter could be supported in args.
    private final ParameterTool setupParams;
    /* The num of queries */
    private final int numQueries;
    /* The window size */
    private final int windowSize;

    protected static final Logger LOG = LoggerFactory.getLogger(TaxiQueryUDDSketch.class);


    public TaxiQueryUDDSketch(ParameterTool setupParams, int numQueries, int windowSize) {
        this.setupParams = setupParams;
        this.numQueries = numQueries;
        this.windowSize = windowSize;
    }


    @Override
    public void run() {
        // Setup Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(setupParams);
        System.out.println("Running Taxi Query");
        LOG.info("Running Taxi Query");

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

        WatermarkStrategy<String> wt = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(0))
            .withTimestampAssigner((event, timestamp) -> Long.parseLong(new JSONObject(event).getString("event_time")));
        DataStream<String> messageStream = env.addSource(
                // Every source is a Kafka Consumer
                new FlinkKafkaConsumer<>(
                    // Different topics for different query queryInstances
                    KAFKA_PREFIX_TOPIC, new SimpleStringSchema(),
                    setupParams.getProperties()).assignTimestampsAndWatermarks(wt).setStartFromEarliest()).name("Source")
            .setParallelism(1);
        // Chain all operators before a watermark is emitted.
        //.disableChaining();

        messageStream
            // Parse the JSON string from Kafka as an ad
            .map(new DeserializeMessageFromKafka()).name("DeserializeInput ").disableChaining()
            .<Tuple7<String, String, String, String, String, String, Boolean>>project(10, 2, 3, 4, 5, 11, 13)
            // 10 - total_amount
            // 2 - vendor_id
            // 3 - pickup_datetime
            // 4 - payment_type
            // 5 - fare_amount
            .name("project ")
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
            .aggregate(new WindowAdsAggregatorMSketch(windowSize))
            .name("DeserializeInput ")
            .name("Window")
            .writeAsText("results-nyt-udds.txt", FileSystem.WriteMode.OVERWRITE);
    }


    private static class DeserializeMessageFromKafka implements
        MapFunction<String, Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, Boolean>> {

        @Override
        public Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, Boolean> map(
            String input) {
            JSONObject obj = new JSONObject(input);
            Boolean fake = false;
            if (obj.has("fake")) {
                fake = true;
            }
            return new Tuple14<>(
                obj.getString("medallion"), // 0
                obj.getString("hack_license"), // 1
                obj.getString("vendor_id"), // 2
                obj.getString("pickup_datetime"), // 3
                obj.getString("payment_type"), // 4
                obj.getString("fare_amount"), // 5
                obj.getString("surcharge"), // 6
                obj.getString("mta_tax"), // 7
                obj.getString("tip_amount"), // 8
                obj.getString("tolls_amount"), // 9
                obj.getString("total_amount"), // 10
                obj.getString("event_time"), // 11
                String.valueOf(System.currentTimeMillis()), // 12 ingestion_time
                fake); // 13
        }
    }

    public static Double percentile(ArrayList<Double> sort_values, double percentile) {
        Preconditions.checkArgument(percentile > 0);
        Preconditions.checkArgument(percentile < 100);
        int index = (int) Math.ceil(percentile / 100.0 * sort_values.size());
        return sort_values.get(index - 1);
    }

    private static class WindowAdsAggregatorMSketch implements
        AggregateFunction<Tuple7<String, String, String, String, String, String, Boolean>, Tuple2<Long, UniformDDSketch>, Tuple2<Long, ArrayList<Double>>> {

        double[] percentiles = {.01, .05, .25, .50, .75, .90, .95, .98, .99};
        int windowSizeAgg;

        public WindowAdsAggregatorMSketch(int windowSize) {
            this.windowSizeAgg = windowSize * 1000; // convert to milliseconds
        }

        @Override
        public Tuple2<Long, UniformDDSketch> createAccumulator() {
            double relativeAccuracy = 0.01;
            int numCollapses = 12;
            double initialAccuracy = Math.tanh(FastMath.atanh(relativeAccuracy) / Math.pow(2.0, numCollapses - 1));
            UniformDDSketch sketch = new UniformDDSketch(1024, initialAccuracy);
            return new Tuple2<>(0L, sketch);
        }

        @Override
        public Tuple2<Long, UniformDDSketch> add(Tuple7<String, String, String, String, String, String, Boolean> value,
                                                 Tuple2<Long, UniformDDSketch> accumulator) {
            accumulator.f1.accept(parseDouble(value.f0));
            accumulator.f0 = Long.parseLong(value.f5) / windowSizeAgg;
            return accumulator;
        }

        @Override
        public Tuple2<Long, UniformDDSketch> merge(Tuple2<Long, UniformDDSketch> acc0,
                                                   Tuple2<Long, UniformDDSketch> acc1) {
            acc0.f1.mergeWith(acc1.f1);
            return acc0;
        }

        @Override
        public Tuple2<Long, ArrayList<Double>> getResult(Tuple2<Long, UniformDDSketch> accumulator) {
            long start = System.nanoTime();
            Tuple2<Long, ArrayList<Double>> ret_tuple = new Tuple2<>();
            ret_tuple.f0 = accumulator.f0;
            ret_tuple.f1 = new ArrayList<>();

            double[] results = accumulator.f1.getValuesAtQuantiles(percentiles);

            for (double d : results) {
                ret_tuple.f1.add(round(d, 4));
            }

            long end = System.nanoTime();
            long elapsed_time = end - start;
            System.out.println("Retrieving result took " + elapsed_time / 1000 + " microseconds");
            System.out.println("Retrieving result took " + elapsed_time + " nanoseconds");
            LOG.info("Retrieving result took " + elapsed_time / 1000 + " microseconds");
            LOG.info("Retrieving result took " + elapsed_time + " nanoseconds");
            ret_tuple.f1.add((double) elapsed_time);
            return ret_tuple;
        }

        private double round(double value, int places) {
            if (places < 0) {
                throw new IllegalArgumentException();
            }

            BigDecimal bd = new BigDecimal(Double.toString(value));
            bd = bd.setScale(places, RoundingMode.HALF_UP);
            return bd.doubleValue();
        }
    }
}
