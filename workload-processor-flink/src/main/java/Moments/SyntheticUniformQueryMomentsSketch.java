package Moments;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import org.apache.flink.shaded.guava18.com.google.common.base.Preconditions;

import com.github.stanfordfuturedata.momentsketch.SimpleMomentSketch;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;

public class SyntheticUniformQueryMomentsSketch implements Runnable {

    /* The Kafka topic the source operators are pulling the results from */
    private final String KAFKA_PREFIX_TOPIC = "synu-events";
    /* The Job Parameters */
    //This class provides simple utility methods for reading and parsing program
    // arguments from different sources.
    // Only single value parameter could be supported in args.
    private final ParameterTool setupParams;
    /* The num of queries */
    private final int numQueries;
    /* The window size */
    private final int windowSize;

    protected static final Logger LOG = LoggerFactory.getLogger(SyntheticUniformQueryMomentsSketch.class);


    public SyntheticUniformQueryMomentsSketch(
        ParameterTool setupParams, int numQueries, int windowSize) {
        this.setupParams = setupParams;
        this.numQueries = numQueries;
        this.windowSize = windowSize;
    }


    @Override
    public void run() {
        // Setup Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(setupParams);
        System.out.println("Running Synthetic Uniform Moments Query");
        LOG.info("Running Synthetic Uniform Moments Query");

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
                    KAFKA_PREFIX_TOPIC,
                    new SimpleStringSchema(),
                    setupParams.getProperties()).assignTimestampsAndWatermarks(wt).setStartFromEarliest())
            .name("Source").setParallelism(1);
        // Chain all operators before a watermark is emitted.
        //.disableChaining();

        messageStream
            // Parse the JSON string from Kafka as an ad
            .map(new DeserializeMessageFromKafka())
            .name("DeserializeInput ")
            .disableChaining()
            .name("project ")
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
            .aggregate(new WindowAdsAggregatorMSketch())
            .name("DeserializeInput ")
            .name("Window")
            .writeAsText("results-synu-moments.txt", FileSystem.WriteMode.OVERWRITE);
    }

    private static class DeserializeMessageFromKafka implements
        MapFunction<String,
            Tuple3<String, String, String>> {

        @Override
        public Tuple3<String, String, String> map(String input) {
            JSONObject obj = new JSONObject(input);
            return new Tuple3<>(
                obj.getString("uniform_value"), // 0
                obj.getString("event_time"), // 1
                String.valueOf(System.currentTimeMillis()) // 2 ingestion_time
            );
        }
    }

    public static Double percentile(ArrayList<Double> sort_values, double percentile) {
        Preconditions.checkArgument(percentile > 0);
        Preconditions.checkArgument(percentile < 100);
        int index = (int) Math.ceil(percentile / 100.0 * sort_values.size());
        return sort_values.get(index - 1);
    }

    private class WindowAdsAggregatorMSketch implements AggregateFunction<
        Tuple3<String, String, String>,
        Tuple2<Long, SimpleMomentSketch>,
        Tuple2<Long, ArrayList<Double>>> {

        double[] percentiles = {0.01, 0.05, 0.25, 0.5, 0.75, 0.90, 0.95, 0.98, 0.99};


        @Override
        public Tuple2<Long, SimpleMomentSketch> createAccumulator() {
            SimpleMomentSketch msketch = new SimpleMomentSketch(12);
            msketch.setCompressed(true);
            return new Tuple2<>(0L, msketch);
        }

        @Override
        public Tuple2<Long, SimpleMomentSketch> add(Tuple3<String, String, String> value,
                                                    Tuple2<Long, SimpleMomentSketch> accumulator) {
            accumulator.f1.add(Double.parseDouble(value.f0)); // f0 is uniform
            accumulator.f0 = Long.parseLong(value.f1) / windowSize;
            return accumulator;
        }

        @Override
        public Tuple2<Long, SimpleMomentSketch> merge(Tuple2<Long, SimpleMomentSketch> acc0,
                                                      Tuple2<Long, SimpleMomentSketch> acc1) {
            acc0.f1.merge(acc1.f1);
            return acc0;
        }

        @Override
        public Tuple2<Long, ArrayList<Double>> getResult(Tuple2<Long, SimpleMomentSketch> accumulator) {
            long start = System.nanoTime();
            Tuple2<Long, ArrayList<Double>> ret_tuple = new Tuple2<>();
            ret_tuple.f0 = accumulator.f0;
            ret_tuple.f1 = new ArrayList<>();

            System.out.println("Moments Struct");
            System.out.println(accumulator.f1);

            double[] results = accumulator.f1.getQuantiles(percentiles);

            for (double d : results) {
                ret_tuple.f1.add(round(d, 4));
            }

            long end = System.nanoTime();
            long elapsed_time = end - start;
            System.out.println("Retrieving result took " + elapsed_time + "nanoseconds");
            LOG.info("Retrieving result took " + elapsed_time + "nanoseconds");
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
