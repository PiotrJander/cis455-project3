package edu.upenn.cis455.mapreduce.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.bolt.MapBolt;
import edu.upenn.cis.stormlite.bolt.ReduceBolt;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.tuple.Fields;
import test.edu.upenn.cis.stormlite.PrintBolt;
import test.edu.upenn.cis.stormlite.mapreduce.WordFileSpout;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class MasterLaunch {

    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String MAP_BOLT = "MAP_BOLT";
    private static final String REDUCE_BOLT = "REDUCE_BOLT";
    private static final String PRINT_BOLT = "PRINT_BOLT";

    private final static String CLASS_NAME = "class_name";
    private final static String INPUT_DIR = "input_dir";
    private final static String OUTPUT_DIR = "output_dir";
    private final static String MAP_THREADS = "map_threads";
    private final static String REDUCE_THREADS = "reduce_threads";

    private final static List<String> params = Arrays.asList(CLASS_NAME, INPUT_DIR, OUTPUT_DIR, MAP_THREADS, REDUCE_THREADS);

    static void postLaunch(
            HttpServletRequest request,
            HttpServletResponse response,
            WorkersMap workers
    ) throws IOException {
        HashMap<String, Object> fields = new HashMap<>();

        params.forEach(param -> {
            fields.put(param, request.getParameter(param));
        });

        boolean success = normalizeLaunch(response, fields);
        if (success) {
            sendDefineJob(request, fields, workers);
            sendRunJob();
        }
    }

    private static Config makeConfig(
            HttpServletRequest request,
            HashMap<String, Object> fields,
            WorkersMap workers
    ) {
        Config config = new Config();

        // TODO based on status


        config.put("workerList", "[127.0.0.1:8000,127.0.0.1:8001]");

//        if (args.length >= 1) {
//            config.put("workerIndex", args[0]);
//        } else
//            config.put("workerIndex", "0");

        // TODO resolve job naming
        config.put("job", "MyJob1");

        config.put("master", request.getLocalAddr() + ":" + request.getLocalPort());

        config.put("mapClass", (String) fields.get(CLASS_NAME));
        config.put("reduceClass", (String) fields.get(CLASS_NAME));

        config.put("spoutExecutors", "1");
        config.put("mapExecutors", (String) fields.get(MAP_THREADS));
        config.put("reduceExecutors", (String) fields.get(REDUCE_THREADS));
        return config;
    }

    private static void sendDefineJob(
            HttpServletRequest request,
            HashMap<String, Object> fields,
            WorkersMap workers
    ) throws IOException {
        Config config = makeConfig(request, fields, workers);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(
                WORD_SPOUT,
                new WordFileSpout(),
                Integer.valueOf(config.get("spoutExecutors"))
        );

        builder.setBolt(
                MAP_BOLT,
                new MapBolt(),
                Integer.valueOf(config.get("mapExecutors"))
        ).fieldsGrouping(WORD_SPOUT, new Fields("value"));

        builder.setBolt(
                REDUCE_BOLT,
                new ReduceBolt(),
                Integer.valueOf(config.get("reduceExecutors"))
        ).fieldsGrouping(MAP_BOLT, new Fields("key"));

        builder.setBolt(
                PRINT_BOLT,
                new PrintBolt(),
                1
        ).firstGrouping(REDUCE_BOLT);

        Topology topo = builder.createTopology();
        WorkerJob job = new WorkerJob(topo, config);
        ObjectMapper mapper = new ObjectMapper();
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

        // now this sends to each worker
//        try {
//            String[] workers = WorkerHelper.getWorkers(config);
//
//            int i = 0;
//            for (String dest : workers) {
//                config.put("workerIndex", String.valueOf(i++));
//                if (sendJob(dest, "POST", config, "definejob",
//                        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job)).getResponseCode() !=
//                        HttpURLConnection.HTTP_OK) {
//                    throw new RuntimeException("Job definition request failed");
//                }
//            }
//            for (String dest : workers) {
//                if (sendJob(dest, "POST", config, "runjob", "").getResponseCode() !=
//                        HttpURLConnection.HTTP_OK) {
//                    throw new RuntimeException("Job execution request failed");
//                }
//            }
//        } catch (JsonProcessingException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//            System.exit(0);
//        }
    }

    private static HttpURLConnection sendJob(String dest, String post, Config config, String runjob, String s) {
        return null;
    }

    private static void sendRunJob() {

    }

    private static boolean normalizeLaunch(HttpServletResponse response, HashMap<String, Object> fields) throws IOException {
        return false;
    }
}
