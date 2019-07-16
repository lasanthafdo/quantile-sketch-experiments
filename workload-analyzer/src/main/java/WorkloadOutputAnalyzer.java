import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class WorkloadOutputAnalyzer {

    public static void main(String[] args) throws Exception {
        String[] parts;

        for (String expName : args) {
            BufferedReader br = new BufferedReader(new FileReader(new File(expName + "_output.txt")));
            //PrintWriter pr = new PrintWriter(new File(expName + "_analysis.txt"));

            List<Event> ingestionList = new ArrayList<>();
            List<Event> aggregationList = new ArrayList<>();

            long seqNumber = 0;
            long numOfMaterializations = 0;

            int numberOfUnecessaryEventsProcessedAfterWatermarkArrival = 0;
            Materialization currMaterialization = null;
            long totalEvents = 0;

            RestStatAnalyzer.getStatistics(expName);
            while (true) {
                String line = br.readLine();
                if (line == null)
                    break;

                totalEvents++;

                parts = line.split("\t");
                if (parts[0].equalsIgnoreCase("ingestion")) {
                    ingestionList.add(new Ingestion(seqNumber, parts[1], Long.parseLong(parts[2]), Long.parseLong(parts[3])));
                } else if (parts[0].equalsIgnoreCase("aggregation")) {
                    aggregationList.add(new Aggregation(seqNumber, parts[1], Long.parseLong(parts[2]), Long.parseLong(parts[3])));
                } else if (parts[0].equalsIgnoreCase("sink")) {
                    currMaterialization = new Materialization(seqNumber, Long.parseLong(parts[1]));
                    numOfMaterializations++;
                }
                seqNumber++;

                if (currMaterialization != null) {
                    System.out.println(currMaterialization.watermark);
                    numberOfUnecessaryEventsProcessedAfterWatermarkArrival += (findUnecessaryEventsInOperator(currMaterialization, ingestionList) * 3);
                    numberOfUnecessaryEventsProcessedAfterWatermarkArrival += findUnecessaryEventsInOperator(currMaterialization, aggregationList);

                    ingestionList = cleanArray(currMaterialization, ingestionList);
                    aggregationList = cleanArray(currMaterialization, aggregationList);
                }

                currMaterialization = null;
            }

            if(numOfMaterializations == 0){
                numOfMaterializations++;
            }

            //pr.println(expName + ":\t" + totalEvents + ":\t" + numOfMaterializations + "\t" + (numberOfUnecessaryEventsProcessedAfterWatermarkArrival / numOfMaterializations * 1.0));
            //pr.flush();
            //pr.close();
        }
    }

    private static List<Event> cleanArray(Materialization materialization, List<Event> eventList) {
        List<Event> newList = new ArrayList<>();
        int indexOrder = 0;
        for (int i = 0; i < eventList.size(); i++) {
            if (eventList.get(i).watermark == materialization.watermark) {
                indexOrder = i;
                break;
            }
        }

        for (int i = indexOrder; i < eventList.size(); i++) {
            newList.add(eventList.get(i));
        }

        return newList;

    }


    private static int findUnecessaryEventsInOperator(Materialization materialization, List<Event> ingestionList) {
        int ret = 0;
        // Find in first operator
        int indexOrder = 0;
        for (int i = 0; i < ingestionList.size(); i++) {
            if (ingestionList.get(i).watermark == materialization.watermark) {
                indexOrder = i;
                break;
            }
        }

        assert indexOrder != 0;

        // How many events were processed in Operator 1 after watermark ingestion?
        for (int i = indexOrder; i < ingestionList.size(); i++) {
            if (materialization.seqNumber > ingestionList.get(i).seqNumber) {
                ret++;
            }
        }
        return ret;
    }

    private static abstract class Event {
        long seqNumber;
        long watermark;

        Event(long seqNumber, long watermark) {
            this.seqNumber = seqNumber;
            this.watermark = watermark;
        }
    }

    private static class Ingestion extends Event {
        String id;
        long ingestionTime;

        Ingestion(long seqNumber, String id, long ingestionTime, long watermark) {
            super(seqNumber, watermark);
            this.id = id;
            this.ingestionTime = ingestionTime;
        }
    }

    private static class Aggregation extends Event {
        String id;
        long ingestionTime;

        Aggregation(long seqNumber, String id, long ingestionTime, long watermark) {
            super(seqNumber, watermark);
            this.id = id;
            this.ingestionTime = ingestionTime;
        }
    }

    private static class Materialization extends Event {

        Materialization(long seqNumber, long watermark) {
            super(seqNumber, watermark);
        }
    }
}