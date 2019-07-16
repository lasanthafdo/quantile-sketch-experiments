import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.runtime.tasks.scheduler.StreamTaskSchedulerPolicy;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Map;

public class AdvertisingQueryWindowSingleNode {


    public static void main(String[] args) throws Exception {
        // Setup parameters
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Map setupMap = Utils.findAndReadConfigFile(parameterTool.getRequired("setup"));
        ParameterTool setupParams = ParameterTool.fromMap(Utils.getFlinkConfs(setupMap));

        Map experimentMap = Utils.findAndReadConfigFile(parameterTool.getRequired("experiment"));
        int queryInstances = ((Number) experimentMap.getOrDefault("num_instances", 1)).intValue();
        int windowSize = ((Integer) experimentMap.getOrDefault("window_size", 3)).intValue();
        int algorithmIndex = ((Integer) experimentMap.getOrDefault("algorithm_name", 0)).intValue();

        // Setup flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Setup scheduling algorithm
        env.setStreamTaskSchedulerPolicy(StreamTaskSchedulerPolicy.fromIndex(algorithmIndex));

        env.getConfig().setGlobalJobParameters(setupParams);

        // Add queries
        for (int i = 1; i <= queryInstances; i++) {
            addQuery(env, setupParams, i, windowSize);
        }

        // Execute queries
        env.execute();
    }

    private static void addQuery(StreamExecutionEnvironment env, ParameterTool setupParams, int instance, int windowSize) {
        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer<>(
                // Different topics for different query instances
                setupParams.getRequired("topic") + "-" + instance,
                new SimpleStringSchema(),
                setupParams.getProperties())).name("Source (" + instance + ")").startNewChain();

        messageStream
                // Parse the JSON string from Kafka as an ad
                .map(new DeserializeAdsFromkafka()).name("DeserializeInput (" + instance + ")")
                // Filter ads
                //.filter(new FilterAds())
                // Assign timestamps and watermarks
                .assignTimestampsAndWatermarks(new AdsWatermarkAndTimeStampAssigner()).name("TimeStamp (" + instance + ")")
                // perform join with redis data
                .map(new JoinAdWithRedis()).name("JoinWithRedis (" + instance + ")").disableChaining()
                // key by compaignId
                .map(new NameToLowerCase()).name("IdenticalMap 1 (" + instance + ")").disableChaining()

                .map(new NameToLowerCase()).name("IdenticalMap 2 (" + instance + ")").disableChaining()

                .map(new NameToLowerCase()).name("IdenticalMap 3 (" + instance + ")").disableChaining()
                //   .keyBy(0)
                // Collect aggregates in event window of 10 seconds
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize))).aggregate(new WindowAdsAggregator()).name("Window (" + instance + ")").disableChaining()
                // sink function
                .addSink(new PrintCampaignAdClicks(instance)).name("Sink(" + instance + ")").disableChaining();

    }

    private static String convertTimestamp(String timestampString) {
        long timestamp = Long.valueOf(timestampString);
        SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss.SSS");
        return formatter.format(timestamp);
    }

    private static class DeserializeAdsFromkafka implements MapFunction<String, Tuple9<String, String, String, String, String, String, String, String, String>> {

        @Override
        public Tuple9<String, String, String, String, String, String, String, String, String> map(String input) {
            JSONObject obj = new JSONObject(input);

            Tuple9<String, String, String, String, String, String, String, String, String> output = new Tuple9<>(
                    obj.getString("user_id"),
                    obj.getString("page_id"),
                    obj.getString("ad_id"),
                    obj.getString("ad_type"),
                    obj.getString("event_type"),
                    obj.getString("event_time"),
                    obj.getString("ip_address"),
                    obj.getString("watermark_time"),
                    String.valueOf(System.currentTimeMillis())); // ingestion time
            return output;
        }
    }

    private static class FilterAds implements FilterFunction<Tuple9<String, String, String, String, String, String, String, String, String>> {

        @Override
        public boolean filter(Tuple9<String, String, String, String, String, String, String, String, String> ad) {
            return ad.getField(4).equals("view");
        }
    }

    private static class AdsWatermarkAndTimeStampAssigner implements AssignerWithPunctuatedWatermarks<Tuple9<String, String, String, String, String, String, String, String, String>> {
        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Tuple9<String, String, String, String, String, String, String, String, String> lastElement, long extractedTimestamp) {
            long watermarkTime = Long.valueOf(lastElement.getField(7));
            return watermarkTime != 0 ? new Watermark(watermarkTime) : null;
        }

        @Override
        public long extractTimestamp(Tuple9<String, String, String, String, String, String, String, String, String> element, long previousElementTimestamp) {
            return Long.valueOf(element.getField(5));
        }
    }

    private static class JoinAdWithRedis extends RichMapFunction<Tuple9<String, String, String, String, String, String, String, String, String>, Tuple5<String, String, String, String, String>> {

        private RedisAdCampaignCache redisAdCampaignCache;

        @Override
        public void open(Configuration parameters) {
            //initialize jedis
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            this.redisAdCampaignCache = new RedisAdCampaignCache(new Jedis(parameterTool.get("jedis_server")));
        }

        @Override
        public Tuple5<String, String, String, String, String> map(Tuple9<String, String, String, String, String, String, String, String, String> input) {
            String userId = input.getField(0);
            String adId = input.getField(2);

            return new Tuple5<>(
                    redisAdCampaignCache.execute(adId),
                    userId + "," + adId,
                    input.getField(5), // event time
                    input.getField(7), // watermark
                    input.getField(8) // ingestion
            );
        }
    }

    private static class NameToLowerCase extends RichMapFunction<Tuple5<String, String, String, String, String>, Tuple5<String, String, String, String, String>> {

        @Override
        public Tuple5<String, String, String, String, String> map(Tuple5<String, String, String, String, String> input) throws Exception {
            return new Tuple5<>(
                    input.f0,
                    input.f1,
                    input.f2, // event time
                    input.f3, // watermark
                    input.f4 // ingestion
            );
        }
    }

    private static class WindowAdsAggregator implements AggregateFunction<Tuple5<String, String, String, String, String>, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> createAccumulator() {
            return new Tuple2<>("", 0);
        }

        @Override
        public Tuple2<String, Integer> add(Tuple5<String, String, String, String, String> value, Tuple2<String, Integer> accumulator) {
            accumulator.f0 = value.f0;
            accumulator.f1 += 1;
            return accumulator;
        }

        @Override
        public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<String, Integer> merge(Tuple2<String, Integer> acc0, Tuple2<String, Integer> acc1) {
            assert acc0.f0.equalsIgnoreCase(acc1.f0);
            return new Tuple2<>(acc0.f0, acc0.f1 + acc1.f1);
        }
    }

    private static class PrintCampaignAdClicks implements SinkFunction<Tuple2<String, Integer>> {
        private final int instance;

        PrintCampaignAdClicks(int instance) {
            this.instance = instance;
        }

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) {
        }
    }
}