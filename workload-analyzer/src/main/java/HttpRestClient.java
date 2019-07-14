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


public  class HttpRestClient {

  public static void getStatistics(String expName) throws Exception {
      System.out.println(expName + "_statistics.txt");
      PrintWriter pr = new PrintWriter(new File(expName + "_statistics.txt"));
      ArrayList<String> metricsList = new ArrayList( );
      metricsList.add("schedulerOverhead_max");
      metricsList.add("sleepDuration_max");
      for (String metrics: metricsList) {
          System.out.printf("------------metric name: %s----------------------\n", metrics);
          pr.println("Metrics name:" + metrics);
          StringBuffer content = get(metrics);
          pr.println(content);
      }
      pr.flush();
      pr.close();
  }


  public static StringBuffer get(String metricsName) throws Exception {
      System.out.println("---------------init http connect--------------------------");
      URL url = new URL("http://localhost:8081/taskmanagers/metrics?get=" + metricsName);
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("GET");


      System.out.println("---------------get http response--------------------------");
      int status = con.getResponseCode();
      System.out.printf("---------------http response code: %d--------------------------\n", status);
      BufferedReader in = new BufferedReader(
      new InputStreamReader(con.getInputStream()));
      String inputLine;
      StringBuffer content = new StringBuffer();
      while ((inputLine = in.readLine()) != null) {
        content.append(inputLine);
      }

      System.out.println("---------------print http response--------------------------");
      
      System.out.println(content);

      in.close();
      con.disconnect();

      return content;
  }
}

