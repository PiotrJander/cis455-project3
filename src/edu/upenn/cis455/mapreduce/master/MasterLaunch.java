package edu.upenn.cis455.mapreduce.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.bolt.MapBolt;
import edu.upenn.cis.stormlite.bolt.ReduceBolt;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.tuple.Fields;
import test.edu.upenn.cis.stormlite.PrintBolt;
import test.edu.upenn.cis.stormlite.mapreduce.WordFileSpout;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
            Config config = makeConfig(request, fields, workers);
            Topology topology = getTopology(config);
            WorkerJob job = new WorkerJob(topology, config);
            String[] activeWorkers = WorkerHelper.getWorkers(config);
            sendDefineJob(activeWorkers, job);
            sendRunJob(activeWorkers, job);
        }
    }

    private static void sendDefineJob(String[] activeWorkers, WorkerJob job) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

        // TODO possibly add worker index to config
        IntStream.range(0, activeWorkers.length).forEach(i -> {
            try {
                String workerAddress = activeWorkers[i];
                String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job);
                if (sendJob(workerAddress, "POST", "/definejob", json).getResponseCode() != HttpURLConnection.HTTP_OK) {
                    throw new RuntimeException("Job definition request failed");
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        });
    }

    private static void sendRunJob(String[] activeWorkers, WorkerJob job) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

        // TODO possibly add worker index to config
        IntStream.range(0, activeWorkers.length).forEach(i -> {
            try {
                String workerAddress = activeWorkers[i];
                String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job);
                if (sendJob(workerAddress, "POST", "/runjob", json).getResponseCode() != HttpURLConnection.HTTP_OK) {
                    throw new RuntimeException("Job execution request failed");
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        });
    }

    private static HttpURLConnection sendJob(String workerAddress, String method, String path, String parameters) throws IOException {
        URL url = new URL(new URL(workerAddress), path);

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod(method);

        if (method.equals("POST")) {
            conn.setRequestProperty("Content-Type", "application/json");
            OutputStream os = conn.getOutputStream();
            byte[] toSend = parameters.getBytes();
            os.write(toSend);
            os.flush();
        } else {
            conn.getOutputStream();
        }

        return conn;
    }

    private static Config makeConfig(HttpServletRequest request, HashMap<String, Object> fields, WorkersMap workers) {
        Config config = new Config();

        String activeWorkers = workers.getActiveWorkers()
                .map(worker -> (String) worker.get("id"))
                .collect(Collectors.joining(","));
        config.put("workerList", "[" + activeWorkers + "]");

        // TODO resolve job naming
        config.put("job", "MyJob1");

        // TODO ensure this is correct
        config.put("master", request.getLocalAddr() + ":" + request.getLocalPort());

        config.put("mapClass", (String) fields.get(CLASS_NAME));
        config.put("reduceClass", (String) fields.get(CLASS_NAME));

        config.put("spoutExecutors", "1");
        config.put("mapExecutors", (String) fields.get(MAP_THREADS));
        config.put("reduceExecutors", (String) fields.get(REDUCE_THREADS));
        return config;
    }

    private static Topology getTopology(Config config) {
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

        return builder.createTopology();
    }

    private static boolean normalizeLaunch(HttpServletResponse response, HashMap<String, Object> fields) throws IOException {
        return false;
    }
}
