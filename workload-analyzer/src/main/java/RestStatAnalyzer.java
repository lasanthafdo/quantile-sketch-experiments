import java.net.HttpURLConnection;
import java.net.URL;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

import java.io.File;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.Thread;

import org.json.JSONObject;


public  class RestStatAnalyzer {
    static final ArrayList<String> statList = new ArrayList< >();
    static final ArrayList<String> metricsList = new ArrayList< >();  

    static {
        statList.add("count");
        statList.add("min");
        statList.add("max");
        statList.add("stddev");
        metricsList.add("numOfCyclesCount");
        metricsList.add("schedulerOverhead");
        metricsList.add("sleepDuration");
        metricsList.add("numberOfRunningTasks");
        metricsList.add("numOfEventsOfAllTasksHisto");
        metricsList.add("numOfEventsOfScheduledTasksHisto");
        metricsList.add("numOfEventsProcessedHisto");
        metricsList.add("starvationHisto");
    }


    public static void getStatistics(String expName) throws Exception {
        final PrintWriter pr = new PrintWriter(new File(expName + "_statistics.txt"));
        StringBuffer refresh = get("");

        for (String metrics: metricsList) {
            switch(metrics) {
                case "schedulerOverhead":
                    pr.println("scheduler overhead:");
                    break;
                case "sleepDuration":
                    pr.println("sleep duration per cycle:");
                    break;
                case "numOfEventsOfAllTasksHisto": 
                    pr.println("num of events of all tasks per cycle:");
                    break;
                case "numberOfRunningTasks": 
                    pr.println("num of scheduled tasks per cycle:");
                    break;
                case "numOfEventsOfScheduledTasksHisto": 
                    pr.println("num of events of scheduled tasks per cycle:");
                    break;
                case "numOfEventsProcessedHisto": 
                    pr.println("num of events of processed tasks per cycle:");
                    break;
                case "starvationHisto": 
                    pr.println("starvation:");
                    break;
                case "numOfCyclesCount":
                    StringBuffer content = get(metrics);
                    String jsonString = content.substring(1, content.length() - 1);
                    JSONObject json = new JSONObject(jsonString);
                    pr.println("numOfCyclesCount: " + json.get("avg"));
                    pr.println();
                    continue;
            }
            for (String stat: statList) {
                StringBuffer content = get(metrics + '_' + stat);
                String jsonString = content.substring(1, content.length() - 1);
                JSONObject json = new JSONObject(jsonString);
                pr.println(stat + ": " + json.get("avg"));
            }
            pr.println();
        }
        pr.flush();
        pr.close();
    }



  public static StringBuffer get(String metricsName) throws Exception {
      URL url = new URL("http://localhost:8081/taskmanagers/metrics?get=" + metricsName  + "&agg=avg");
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("GET");

      int status = con.getResponseCode();
      BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
      String inputLine;
      StringBuffer content = new StringBuffer();
      while ((inputLine = in.readLine()) != null) {
          content.append(inputLine);
      }

      in.close();
      con.disconnect();
      Thread.sleep(1000);
      return content;
  }
}

