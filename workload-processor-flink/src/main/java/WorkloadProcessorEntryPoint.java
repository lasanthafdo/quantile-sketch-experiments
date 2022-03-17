import org.apache.flink.api.java.utils.ParameterTool;

import DDSketch.PowerQueryDDSketch;
import DDSketch.SyntheticParetoQueryDDSketch;
import DDSketch.SyntheticUniformQueryDDSketch;
import DDSketch.TaxiQueryDDSketch;
import DDSketchCollapsing.PowerQueryDDSketchCollapsing;
import DDSketchCollapsing.SyntheticParetoQueryDDSketchCollapsing;
import DDSketchCollapsing.SyntheticUniformQueryDDSketchCollapsing;
import DDSketchCollapsing.TaxiQueryDDSketchCollapsing;
import KLLSketch.PowerQueryKLLSketch;
import KLLSketch.SyntheticParetoQueryKLLSketch;
import KLLSketch.SyntheticUniformQueryKLLSketch;
import KLLSketch.TaxiQueryKLLSketch;
import LRB.LinearRoadQuery;
import Moments.PowerQueryMomentsSketch;
import Moments.SyntheticParetoQueryMomentsSketch;
import Moments.SyntheticUniformQueryMomentsSketch;
import Moments.TaxiQueryMomentsSketch;
import REQSketch.PowerQueryREQSketch;
import REQSketch.SyntheticParetoQueryREQSketch;
import REQSketch.SyntheticUniformQueryREQSketch;
import REQSketch.TaxiQueryREQSketch;
import UDDSketch.PowerQueryUDDSketch;
import UDDSketch.SyntheticParetoQueryUDDSketch;
import UDDSketch.SyntheticUniformQueryUDDSketch;
import UDDSketch.TaxiQueryUDDSketch;
import YSB.AdvertisingQuery;

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
        } else if (workloadType.equalsIgnoreCase("nyt")) {
            createNYTInstances(setupParams, experimentMap);
        } else if (workloadType.equalsIgnoreCase("synp")) {
            createSYNPInstances(setupParams, experimentMap);
        } else if (workloadType.equalsIgnoreCase("synu")) {
            createSYNUInstances(setupParams, experimentMap);
        } else if (workloadType.equalsIgnoreCase("power")) {
            createPowerInstances(setupParams, experimentMap);
        } else {
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

            if (algorithm.equals("moments")) {
                TaxiQueryMomentsSketch taxiQuery = new TaxiQueryMomentsSketch(setupParams, numQueries, windowSize);
                taxiQuery.run();
            } else if (algorithm.equals("ddsketch")) {
                TaxiQueryDDSketch taxiQueryDDSketch = new TaxiQueryDDSketch(setupParams, numQueries, windowSize);
                taxiQueryDDSketch.run();
            } else if (algorithm.equals("ddsketch_collapsing")) {
                TaxiQueryDDSketchCollapsing taxiQueryDDSketchCollapsing =
                    new TaxiQueryDDSketchCollapsing(setupParams, numQueries, windowSize);
                taxiQueryDDSketchCollapsing.run();
            } else if (algorithm.equals("kllsketch")) {
                TaxiQueryKLLSketch taxiQueryKLLSketch = new TaxiQueryKLLSketch(setupParams, numQueries, windowSize);
                taxiQueryKLLSketch.run();
            } else if (algorithm.equals("reqsketch")) {
                TaxiQueryREQSketch taxiQueryREQSketch = new TaxiQueryREQSketch(setupParams, numQueries, windowSize);
                taxiQueryREQSketch.run();
            } else if (algorithm.equals("uddsketch")) {
                TaxiQueryUDDSketch taxiQueryUDDSketch = new TaxiQueryUDDSketch(setupParams, numQueries, windowSize);
                taxiQueryUDDSketch.run();
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

            if (algorithm.equals("moments")) {
                PowerQueryMomentsSketch powerQuery = new PowerQueryMomentsSketch(setupParams, numQueries, windowSize);
                powerQuery.run();
            } else if (algorithm.equals("ddsketch")) {
                PowerQueryDDSketch powerQuery = new PowerQueryDDSketch(setupParams, numQueries, windowSize);
                powerQuery.run();
            } else if (algorithm.equals("ddsketch_collapsing")) {
                PowerQueryDDSketchCollapsing powerQuery =
                    new PowerQueryDDSketchCollapsing(setupParams, numQueries, windowSize);
                powerQuery.run();
            } else if (algorithm.equals("kllsketch")) {
                PowerQueryKLLSketch powerQuery = new PowerQueryKLLSketch(setupParams, numQueries, windowSize);
                powerQuery.run();
            } else if (algorithm.equals("reqsketch")) {
                PowerQueryREQSketch powerQuery = new PowerQueryREQSketch(setupParams, numQueries, windowSize);
                powerQuery.run();
            } else if (algorithm.equals("uddsketch")) {
                PowerQueryUDDSketch powerQuery = new PowerQueryUDDSketch(setupParams, numQueries, windowSize);
                powerQuery.run();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createSYNPInstances(ParameterTool setupParams, Map experimentMap) {
        try {
            // Number of queries
            int numQueries = ((Number) experimentMap.getOrDefault("num_instances", 1)).intValue();

            // Window size
            int windowSize = ((Integer) experimentMap.getOrDefault("window_size", 3)).intValue();

            String algorithm = experimentMap.getOrDefault("algorithm", null).toString();

            // Run YSB query
            if (algorithm.equals("moments")) {
                SyntheticParetoQueryMomentsSketch
                    synQuery = new SyntheticParetoQueryMomentsSketch(setupParams, numQueries, windowSize);
                synQuery.run();
            } else if (algorithm.equals("ddsketch")) {
                SyntheticParetoQueryDDSketch
                    synQuery = new SyntheticParetoQueryDDSketch(setupParams, numQueries, windowSize);
                synQuery.run();
            } else if (algorithm.equals("ddsketch_collapsing")) {
                SyntheticParetoQueryDDSketchCollapsing
                    synQuery = new SyntheticParetoQueryDDSketchCollapsing(setupParams, numQueries, windowSize);
                synQuery.run();
            } else if (algorithm.equals("kllsketch")) {
                SyntheticParetoQueryKLLSketch
                    synQuery = new SyntheticParetoQueryKLLSketch(setupParams, numQueries, windowSize);
                synQuery.run();
            } else if (algorithm.equals("reqsketch")) {
                SyntheticParetoQueryREQSketch
                    synQuery = new SyntheticParetoQueryREQSketch(setupParams, numQueries, windowSize);
                synQuery.run();
            } else if (algorithm.equals("uddsketch")) {
                SyntheticParetoQueryUDDSketch
                    synQuery = new SyntheticParetoQueryUDDSketch(setupParams, numQueries, windowSize);
                synQuery.run();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createSYNUInstances(ParameterTool setupParams, Map experimentMap) {
        try {
            // Number of queries
            int numQueries = ((Number) experimentMap.getOrDefault("num_instances", 1)).intValue();

            // Window size
            int windowSize = ((Integer) experimentMap.getOrDefault("window_size", 3)).intValue();

            String algorithm = experimentMap.getOrDefault("algorithm", null).toString();

            // Run YSB query
            if (algorithm.equals("moments")) {
                SyntheticUniformQueryMomentsSketch
                    synQuery = new SyntheticUniformQueryMomentsSketch(setupParams, numQueries, windowSize);
                synQuery.run();
            } else if (algorithm.equals("ddsketch")) {
                SyntheticUniformQueryDDSketch
                    synQuery = new SyntheticUniformQueryDDSketch(setupParams, numQueries, windowSize);
                synQuery.run();
            } else if (algorithm.equals("ddsketch_collapsing")) {
                SyntheticUniformQueryDDSketchCollapsing
                    synQuery = new SyntheticUniformQueryDDSketchCollapsing(setupParams, numQueries, windowSize);
                synQuery.run();
            } else if (algorithm.equals("kllsketch")) {
                SyntheticUniformQueryKLLSketch
                    synQuery = new SyntheticUniformQueryKLLSketch(setupParams, numQueries, windowSize);
                synQuery.run();
            } else if (algorithm.equals("reqsketch")) {
                SyntheticUniformQueryREQSketch
                    synQuery = new SyntheticUniformQueryREQSketch(setupParams, numQueries, windowSize);
                synQuery.run();
            } else if (algorithm.equals("uddsketch")) {
                SyntheticUniformQueryUDDSketch
                    synQuery = new SyntheticUniformQueryUDDSketch(setupParams, numQueries, windowSize);
                synQuery.run();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
