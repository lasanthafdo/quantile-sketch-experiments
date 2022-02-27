package KLLSketch;

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.guava18.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.Double.parseDouble;

public class SyntheticQueryKLLSketch implements Runnable {

    /* The Kafka topic the source operators are pulling the results from */
    private final String KAFKA_PREFIX_TOPIC = "syn-events";
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

    protected static final Logger LOG = LoggerFactory.getLogger(SyntheticQueryKLLSketch.class);


    public SyntheticQueryKLLSketch(
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
        System.out.println("Running Synthetic Query");
        LOG.info("Running Synthetic Query");

        // Add queries
        addQuery(env);

        // Execute queries
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void addQuery(StreamExecutionEnvironment env){

        WatermarkStrategy<String> wt = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(0))
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
                .map(new SyntheticQueryKLLSketch.DeserializeFromkafka())
                .name("DeserializeInput ")
                .disableChaining()
                .name("project ")
                //.keyBy(0)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .aggregate(new SyntheticQueryKLLSketch.WindowAdsAggregatorMSketch())
                .name("DeserializeInput ")
                .name("Window")
                .writeAsText("results-syn-kll.txt", FileSystem.WriteMode.OVERWRITE);
        // sink function
        //.addSink(new PrintCampaignAdClicks())
        //.name("Sink(" + queryInstance + ")");
    }


    private class DeserializeFromkafka implements
            MapFunction<String,
                    Tuple5<String, String, String, String, String>> {

        @Override
        public Tuple5<String, String, String, String, String> map(String input) {
            JSONObject obj = new JSONObject(input);
            Boolean fake = false;
            if (obj.has("fake")){
                fake = true;
            }
            return new Tuple5<>(
                    obj.getString("pareto_value"), // 0
                    obj.getString("uniform_value"), // 1
                    obj.getString("normal_value"), // 2
                    obj.getString("event_time"), // 3
                    String.valueOf(System.currentTimeMillis()) // 4 ingestion_time
            );
        }
    }

    public static Double percentile(ArrayList<Double> sort_values, double percentile) {
        Preconditions.checkArgument(percentile > 0);
        Preconditions.checkArgument(percentile < 100);
        int index = (int) Math.ceil(percentile / 100.0 * sort_values.size());
        return sort_values.get(index-1);
    }

    private class WindowAdsAggregatorMSketch implements AggregateFunction<
            Tuple5<String, String, String, String, String>,
            Tuple2<Long, KllFloatsSketch>,
            Tuple2<Long, ArrayList<Double>>> {

        double[] percentiles = {.01, .05, .25, .50, .75, .90, .95, .98, .99};

        @Override
        public Tuple2<Long, KllFloatsSketch> createAccumulator() {
            return new Tuple2<>(0L, new KllFloatsSketch(600));
        }

        @Override
        public Tuple2<Long, KllFloatsSketch> add(Tuple5<String, String, String, String, String> value,
                                              Tuple2<Long, KllFloatsSketch> accumulator) {
            accumulator.f1.update(Float.parseFloat(value.f0)); // f0 is pareto, f1 is uniform, f2 is normal
            int WINDOW_SIZE = 30000; // in milliseconds
            accumulator.f0 = Long.parseLong(value.f3)/WINDOW_SIZE;
            return accumulator;
        }

        @Override
        public Tuple2<Long, KllFloatsSketch> merge(Tuple2<Long, KllFloatsSketch> acc0,
                                                Tuple2<Long, KllFloatsSketch> acc1) {
            acc0.f1.merge(acc1.f1);
            return acc0;
        }

        @Override
        public Tuple2<Long, ArrayList<Double>> getResult(Tuple2<Long, KllFloatsSketch> accumulator) {
            long start = System.nanoTime();
            Tuple2<Long, ArrayList<Double>> ret_tuple = new Tuple2<>();
            ret_tuple.f0 = accumulator.f0;
            ret_tuple.f1 = new ArrayList<>();

            System.out.println("KLL Struct");
            System.out.println(accumulator.f1);

            float[] results = accumulator.f1.getQuantiles(percentiles);

            for(float d : results) ret_tuple.f1.add(round(d, 4));

            long end = System.nanoTime();
            long elapsed_time = end - start;
            System.out.println("-------------------------------------------------------------");
            System.out.println("Retrieving result took " + elapsed_time/1000 + "microseconds");
            LOG.info("Retrieving result took " + elapsed_time/1000 + "microseconds");
            ret_tuple.f1.add((double) elapsed_time);
            double numRetained = (double) accumulator.f1.getNumRetained();
            ret_tuple.f1.add(numRetained);
            System.out.println(accumulator.f1.toString());
            System.out.println("-------------------------------------------------------------");
            return ret_tuple;
        }

        private double round(double value, int places) {
            if (places < 0) throw new IllegalArgumentException();

            BigDecimal bd = new BigDecimal(Double.toString(value));
            bd = bd.setScale(places, RoundingMode.HALF_UP);
            return bd.doubleValue();
        }

    }
}
