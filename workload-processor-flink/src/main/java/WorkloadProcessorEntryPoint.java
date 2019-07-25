import LRB.LinearRoadQuery;
import YSB.AdvertisingQuery;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.runtime.tasks.scheduler.StreamTaskSchedulerPolicy;

import java.util.Map;

public class WorkloadProcessorEntryPoint {

    public static void main(String[] args) {

        // Setup parameters
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Map setupMap = Utils.findAndReadConfigFile(parameterTool.getRequired("setup"));
        ParameterTool setupParams = ParameterTool.fromMap(Utils.getFlinkConfs(setupMap));

        Map experimentMap = Utils.findAndReadConfigFile(parameterTool.getRequired("experiment"));

        String workloadType = (String) experimentMap.get("workload_type");
        if (workloadType.equalsIgnoreCase("ysb")) {
            createYSBInstances(setupParams, experimentMap);
        } else if (workloadType.equalsIgnoreCase("lrb")) {
            createLRBInstances(setupParams, experimentMap);
        }
    }

    private static void createYSBInstances(ParameterTool setupParams, Map experimentMap) {
        try {
            // Scheduler policy
            int policyIndex = ((Integer) experimentMap.getOrDefault("policy_index", 0)).intValue();
            StreamTaskSchedulerPolicy policy = StreamTaskSchedulerPolicy.fromIndex(policyIndex);

            // Number of queries
            int numQueries = ((Number) experimentMap.getOrDefault("num_instances", 1)).intValue();

            // Window size
            int windowSize = ((Integer) experimentMap.getOrDefault("window_size", 3)).intValue();

            // Run YSB query
            AdvertisingQuery ysbQuery = new AdvertisingQuery(setupParams, policy, numQueries, windowSize);
            ysbQuery.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createLRBInstances(ParameterTool setupParams, Map experimentMap) {
        try {
            // Scheduler policy
            int policyIndex = ((Integer) experimentMap.getOrDefault("policy_index", 0)).intValue();
            StreamTaskSchedulerPolicy policy = StreamTaskSchedulerPolicy.fromIndex(policyIndex);

            // Number of queries
            int numQueries = ((Number) experimentMap.getOrDefault("num_instances", 1)).intValue();

            // Window size
            int windowSize = ((Integer) experimentMap.getOrDefault("window_size", 3)).intValue();

            // Run LRB query
            LinearRoadQuery lrQuery = new LinearRoadQuery(setupParams, policy, numQueries);
            lrQuery.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}