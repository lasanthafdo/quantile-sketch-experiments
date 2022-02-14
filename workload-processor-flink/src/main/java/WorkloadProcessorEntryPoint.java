import DDSketch.PowerQueryDDSketch;
import DDSketch.SyntheticQueryDDSketch;
import DDSketch.TaxiQueryDDSketch;
import KLLSketch.PowerQueryKLLSketch;
import KLLSketch.SyntheticQueryKLLSketch;
import KLLSketch.TaxiQueryKLLSketch;
import LRB.LinearRoadQuery;
import Moments.PowerQueryMomentsSketch;
import Moments.SyntheticQuery;
import YSB.AdvertisingQuery;
import Moments.TaxiQuery;
import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.runtime.tasks.scheduler.StreamTaskSchedulerPolicy;

import java.util.Map;

public class WorkloadProcessorEntryPoint {

    public static void main(String[] args) {

        // Setup parameters
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Map setupMap = Utils.findAndReadConfigFile(parameterTool.getRequired("setup"));
        ParameterTool setupParams = ParameterTool.fromMap(Utils.getFlinkConfs(setupMap));

        System.out.println(setupParams.toMap().toString());

        Map experimentMap = Utils.findAndReadConfigFile(parameterTool.getRequired("experiment"));

        String workloadType = (String) experimentMap.get("workload_type");
        if (workloadType.equalsIgnoreCase("ysb")) {
            createYSBInstances(setupParams, experimentMap);
        } else if (workloadType.equalsIgnoreCase("lrb")) {
            createLRBInstances(setupParams, experimentMap);
        }else if (workloadType.equalsIgnoreCase("nyt")) {
            createNYTInstances(setupParams, experimentMap);
        }else if (workloadType.equalsIgnoreCase("syn")) {
            createSYNInstances(setupParams, experimentMap);
        }else if (workloadType.equalsIgnoreCase("power")) {
            createPowerInstances(setupParams, experimentMap);
        }else{
            System.out.println("no matching workload type found");
        }
    }

    private static void createYSBInstances(ParameterTool setupParams, Map experimentMap) {
        try {
            // Number of queries
            int numQueries = ((Number) experimentMap.getOrDefault("num_instances", 1)).intValue();

            // Window size
            int windowSize = ((Integer) experimentMap.getOrDefault("window_size", 3)).intValue();

            // Run YSB query
            AdvertisingQuery ysbQuery = new AdvertisingQuery(setupParams, numQueries, windowSize);
            ysbQuery.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createLRBInstances(ParameterTool setupParams, Map experimentMap) {
        try {
            // Number of queries
            int numQueries = ((Number) experimentMap.getOrDefault("num_instances", 1)).intValue();

            // Window size
            int windowSize = ((Integer) experimentMap.getOrDefault("window_size", 3)).intValue();

            // Run LRB query
            LinearRoadQuery lrQuery = new LinearRoadQuery(setupParams, numQueries);
            lrQuery.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createNYTInstances(ParameterTool setupParams, Map experimentMap) {
        try {
            // Number of queries
            int numQueries = ((Number) experimentMap.getOrDefault("num_instances", 1)).intValue();

            // Window size
            int windowSize = ((Integer) experimentMap.getOrDefault("window_size", 3)).intValue();

            String algorithm = experimentMap.getOrDefault("algorithm", null).toString();

            if(algorithm.equals("moments")){
                TaxiQuery taxiQuery = new TaxiQuery(setupParams, numQueries, windowSize);
                taxiQuery.run();
            }else if (algorithm.equals("ddsketch")){
                TaxiQueryDDSketch taxiQueryDDSketch = new TaxiQueryDDSketch(setupParams, numQueries, windowSize);
                taxiQueryDDSketch.run();
            }else if (algorithm.equals("kllsketch")) {
                TaxiQueryKLLSketch taxiQueryDDSketch = new TaxiQueryKLLSketch(setupParams, numQueries, windowSize);
                taxiQueryDDSketch.run();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createPowerInstances(ParameterTool setupParams, Map experimentMap) {
        try {
            // Number of queries
            int numQueries = ((Number) experimentMap.getOrDefault("num_instances", 1)).intValue();

            // Window size
            int windowSize = ((Integer) experimentMap.getOrDefault("window_size", 3)).intValue();

            String algorithm = experimentMap.getOrDefault("algorithm", null).toString();

            if(algorithm.equals("moments")){
                PowerQueryMomentsSketch powerQuery = new PowerQueryMomentsSketch(setupParams, numQueries, windowSize);
                powerQuery.run();
            }else if (algorithm.equals("ddsketch")){
                PowerQueryDDSketch powerQuery = new PowerQueryDDSketch(setupParams, numQueries, windowSize);
                powerQuery.run();
            }else if (algorithm.equals("kllsketch")) {
                PowerQueryKLLSketch powerQuery = new PowerQueryKLLSketch(setupParams, numQueries, windowSize);
                powerQuery.run();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createSYNInstances(ParameterTool setupParams, Map experimentMap) {
        try {
            // Number of queries
            int numQueries = ((Number) experimentMap.getOrDefault("num_instances", 1)).intValue();

            // Window size
            int windowSize = ((Integer) experimentMap.getOrDefault("window_size", 3)).intValue();

            String algorithm = experimentMap.getOrDefault("algorithm", null).toString();

            // Run YSB query
            if (algorithm.equals("moments")){
                SyntheticQuery synQuery = new SyntheticQuery(setupParams, numQueries, windowSize);
                synQuery.run();
            }else if (algorithm.equals("ddsketch")){
                SyntheticQueryDDSketch synQuery = new SyntheticQueryDDSketch(setupParams, numQueries, windowSize);
                synQuery.run();
            }else if (algorithm.equals("kllsketch")){
                SyntheticQueryKLLSketch synQuery = new SyntheticQueryKLLSketch(setupParams, numQueries, windowSize);
                synQuery.run();

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
