package edu.upenn.cis455.mapreduce.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.DistributedCluster;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Tuple;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static spark.Spark.setPort;

/**
 * Simple listener for worker creation 
 * 
 * @author zives
 *
 */
public class WorkerServer {

    private static Logger log = Logger.getLogger(WorkerServer.class);
    private static DistributedCluster cluster = new DistributedCluster();
    private static List<String> topologies = new ArrayList<>();
    private List<TopologyContext> contexts = new ArrayList<>();

    public WorkerServer(int port) throws MalformedURLException {

        log.info("Creating server listener at socket " + port);

        setPort(port);
        final ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        Spark.post(new Route("/definejob") {

            @Override
            public Object handle(Request arg0, Response arg1) {

                WorkerJob workerJob;
                try {
                    workerJob = om.readValue(arg0.body(), WorkerJob.class);

                    try {
                        log.info("Processing job definition request" + workerJob.getConfig().get("job") +
                                " on machine " + workerJob.getConfig().get("workerIndex"));
                        contexts.add(cluster.submitTopology(workerJob.getConfig().get("job"), workerJob.getConfig(),
                                workerJob.getTopology()));

                        synchronized (topologies) {
                            topologies.add(workerJob.getConfig().get("job"));
                        }
                    } catch (ClassNotFoundException e) {
                        // Auto-generated catch block
                        e.printStackTrace();
                    }
                    return "Job launched";
                } catch (IOException e) {
                    e.printStackTrace();

                    // Internal server error
                    arg1.status(500);
                    return e.getMessage();
                }

            }

        });

        Spark.post(new Route("/runjob") {

            @Override
            public Object handle(Request arg0, Response arg1) {
                log.info("Starting job!");
                cluster.startTopology();

                return "Started";
            }
        });

        Spark.post(new Route("/pushdata/:stream") {

            @Override
            public Object handle(Request arg0, Response arg1) {
                try {
                    String stream = arg0.params(":stream");
                    Tuple tuple = om.readValue(arg0.body(), Tuple.class);

                    log.info("Worker received: " + tuple + " for " + stream);

                    // Find the destination stream and route to it
                    StreamRouter router = cluster.getStreamRouter(stream);

                    if (contexts.isEmpty())
                        log.error("No topology context -- were we initialized??");

                    if (!tuple.isEndOfStream())
                        contexts.get(contexts.size() - 1).incSendOutputs(router.getKey(tuple.getValues()));

                    if (tuple.isEndOfStream())
                        router.executeEndOfStreamLocally(contexts.get(contexts.size() - 1));
                    else
                        router.executeLocally(tuple, contexts.get(contexts.size() - 1));

                    return "OK";
                } catch (IOException e) {
                    // Auto-generated catch block
                    e.printStackTrace();

                    arg1.status(500);
                    return e.getMessage();
                }

            }

        });

    }

    public static void createWorker(Map<String, String> config) {
        if (!config.containsKey("workerList"))
            throw new RuntimeException("Worker spout doesn't have list of worker IP addresses/ports");

        if (!config.containsKey("workerIndex"))
            throw new RuntimeException("Worker spout doesn't know its worker ID");
        else {
            String[] addresses = WorkerHelper.getWorkers(config);
            String myAddress = addresses[Integer.valueOf(config.get("workerIndex"))];

            log.info("Initializing worker " + myAddress);

            URL url;
            try {
                url = new URL(myAddress);

                WorkerServer worker = new WorkerServer(url.getPort());
            } catch (MalformedURLException e) {
                // Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static void shutdown() {
        synchronized (topologies) {
            for (String topo : topologies)
                cluster.killTopology(topo);
        }

        cluster.shutdown();
    }

    /**
     * Start the second node; the one running on a different Java virtual machine than the master.
     */
    public static void main(String[] args) {

        BasicConfigurator.configure();

        Config config = new Config();
        config.put("workerList", "[127.0.0.1:8000,127.0.0.1:8001]");

        // start the second (1-th) node
        config.put("workerIndex", "1");

        WorkerServer.createWorker(config);

        // master originally started here

//        System.out.println("Press [Enter] to exit...");
//        (new BufferedReader(new InputStreamReader(System.in))).readLine();
//        WorkerServer.shutdown();
//        System.exit(0);
    }

}
