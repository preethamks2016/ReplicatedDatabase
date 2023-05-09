package com.kv.service.grpc;

import com.opencsv.CSVWriter;
import org.apache.zookeeper.common.StringUtils;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class MetricsDataParser {


        ConsistencyType consistencyType;
        String requestType;
        int numOfRequets;
        int averageReadLatency;
        int averageWriteLatency;
        int totalTimeMillis;
        double throughput;
        int monotonicRetryCount;
        int numNodes;


    public static void main(String[] args) throws IOException {
        final File folder = new File(args[0] + "_nodes");
        Map<String, String> fileNames = new HashMap<>();
        for (final File fileEntry : folder.listFiles()) {
            fileNames.put(fileEntry.getName(), fileEntry.getAbsolutePath());
        }
        System.out.println("Read file names");
        int numNodes = Integer.valueOf(args[0]);
        List<MetricsDataParser> metricsDataParsers = getMetrics(fileNames, numNodes);

        FileWriter outputfile = new FileWriter(args[0] + "_nodes_metrics.csv");
        CSVWriter writer = new CSVWriter(outputfile);
        String[] header = { "consistency_type", "request_type", "num_requests", "avg_read_latency", "avg_write_latency", "total_time", "throughput", "monotonic_retry_count" };
        writer.writeNext(header);
        for(MetricsDataParser metricsDataParser : metricsDataParsers) {
            /*System.out.println("_______________________________________________");
            System.out.println("consistencyType" + metricsDataParser.consistencyType);
            System.out.println("requestType" + metricsDataParser.requestType);
            System.out.println("numOfRequets" + metricsDataParser.numOfRequets);
            System.out.println("averageReadLatency" + metricsDataParser.averageReadLatency);
            System.out.println("averageWriteLatency" + metricsDataParser.averageWriteLatency);
            System.out.println("totalTimeMillis" + metricsDataParser.totalTimeMillis);
            System.out.println("throughput" + metricsDataParser.throughput);
            System.out.println("monotonicRetryCount" + metricsDataParser.monotonicRetryCount);*/

            String[] data = {String.valueOf(metricsDataParser.consistencyType), metricsDataParser.requestType,
                    String.valueOf(metricsDataParser.numOfRequets), String.valueOf(metricsDataParser.averageReadLatency),
                    String.valueOf(metricsDataParser.averageWriteLatency), String.valueOf(metricsDataParser.totalTimeMillis),
                    String.valueOf(metricsDataParser.throughput), String.valueOf(metricsDataParser.monotonicRetryCount)};

            writer.writeNext(data);
        }
        writer.close();
    }

    private static List<MetricsDataParser> getMetrics(Map<String, String> fileNames, int numNodes) {
        List<Integer> numRequqsts = Arrays.asList(1000,3000,5000,1000);
        List<ConsistencyType> consistencyTypes = Arrays.asList(ConsistencyType.STRONG, ConsistencyType.EVENTUAL, ConsistencyType.MONOTONIC_READS, ConsistencyType.READ_MY_WRITES);
        List<String> metricsTypes = Arrays.asList("writeLatencies", "readLatencies", "overallMetrics");
        List<String> loadPolicy = Arrays.asList("random", "round_robin");
        List<String> requestTypes = Arrays.asList("get", "put", "get_put");
        List<MetricsDataParser> metricsDataParsers = new ArrayList<>();
        for(String req : requestTypes)  {
            for(ConsistencyType consistencyType : consistencyTypes) {
                for(Integer num : numRequqsts) {
                    for(String pol : loadPolicy) {
                        String name = "_" + String.valueOf(numNodes) + "_" +String.valueOf(consistencyType) + "_"+ String.valueOf(num) + "_" +pol +"_" + req + ".txt";

                        String readLatencyFile = "readLatencies" +  name;
                        String writeLatencyFile = "writeLatencies" +  name;
                        String overall = "overallMetrics" +  name;
                        //System.out.println("Name -- " + readLatencyFile);
                        if(fileNames.containsKey(readLatencyFile)) {
                            MetricsDataParser metrics = new MetricsDataParser();
                            metrics.requestType = req;
                            metrics.numNodes = numNodes;
                            metrics.consistencyType = consistencyType;
                            if (req.equals("get_put")) {
                                metrics.numOfRequets = num * 2;
                            } else {
                                metrics.numOfRequets = num;
                            }

                            int avgReadLatency = getLatency(fileNames.get(readLatencyFile), num);
                            metrics.averageReadLatency = avgReadLatency;
                            int avgWriteLatency = getLatency(fileNames.get(writeLatencyFile), num);
                            metrics.averageWriteLatency = avgWriteLatency;
                            Map<String, Integer> overallMetrics = getOverallMetrics(fileNames.get(overall));
                            metrics.totalTimeMillis = overallMetrics.get("totalTime");
                            metrics.monotonicRetryCount = overallMetrics.get("monotonicRetryCount");
                            if(metrics.totalTimeMillis !=0) {
                                double sec = metrics.totalTimeMillis /1000.0;
                                metrics.throughput = metrics.numOfRequets / sec;
                            }
                            metricsDataParsers.add(metrics);
                        }
                    }
                }
            }
        }
        return metricsDataParsers;
    }

    private static Map<String, Integer> getOverallMetrics(String overallMetricsFile) {
        File file = new File(overallMetricsFile);
        BufferedReader reader = null;
        Map<String, Integer> overallMetrics = new HashMap<>();
        try {
            reader = new BufferedReader(new FileReader(file));
            String text = null;

            while ((text = reader.readLine()) != null) {
                if(text.contains("Total time - ")) {
                    Integer totalTime = Integer.valueOf(text.split("Total time - ")[1].trim());
                    overallMetrics.put("totalTime", totalTime);
                } else if(text.contains("Monotonic Count - ")) {
                    Integer monCount = Integer.valueOf(text.split("Monotonic Count - ")[1].trim());
                    overallMetrics.put("monotonicRetryCount", monCount);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
            }
        }
        return overallMetrics;
    }

    private static int getLatency(String s, Integer numOfRequests) {
        System.out.println(String.format("file - %s, numRes - %d", s, numOfRequests));

        File file = new File(s);
        List<Integer> latencies = new ArrayList<>();
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            String text = null;

            while ((text = reader.readLine()) != null) {
                latencies.add(Integer.parseInt(text));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
            }
        }

        int sum = latencies.stream().mapToInt(Integer::intValue).sum();
        System.out.println("Sum -- " + sum);
        return sum/numOfRequests;
//print out the list

    }
}
