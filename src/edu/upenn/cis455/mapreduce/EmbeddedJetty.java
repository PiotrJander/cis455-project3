package edu.upenn.cis455.mapreduce;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;
import org.apache.log4j.BasicConfigurator;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AllowSymLinkAliasChecker;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;

public class EmbeddedJetty
{
    public static void main( String[] args ) throws Exception
    {
        BasicConfigurator.configure();
        jetty();
        node(args);
    }

    private static void node(String[] args) throws IOException {

        Config config = new Config();
        config.put("workerList", "[127.0.0.1:8000,127.0.0.1:8001]");

        // start the first (0-th) node along master by default
        config.put("workerIndex", "0");

        WorkerServer.createWorker(config);

        // master originally started here

        // TODO when and how do we exit? destroy on servlet

//        System.out.println("Press [Enter] to exit...");
//        (new BufferedReader(new InputStreamReader(System.in))).readLine();
//        WorkerServer.shutdown();
//        System.exit(0);
    }

    private static void jetty() throws Exception {

        // Create a basic jetty server object that will listen on port 8080.
        // Note that if you set this to port 0 then a randomly available port
        // will be assigned that you can either look in the logs for the port,
        // or programmatically obtain it for use in test cases.
        Server server = new Server(8080);

        // Setup JMX
        MBeanContainer mbContainer = new MBeanContainer(
                ManagementFactory.getPlatformMBeanServer());
        server.addBean(mbContainer);

        // The WebAppContext is the entity that controls the environment in
        // which a web application lives and breathes. In this example the
        // context path is being set to "/" so it is suitable for serving root
        // context requests and then we see it setting the location of the war.
        // A whole host of other configurations are available, ranging from
        // configuring to support annotation scanning in the webapp (through
        // PlusConfiguration) to choosing where the webapp will unpack itself.
        WebAppContext webapp = new WebAppContext();
        webapp.setParentLoaderPriority(true);
        webapp.setContextPath("/");
        File warFile = new File("master.war");
        webapp.setWar(warFile.getAbsolutePath());
        webapp.addAliasCheck(new AllowSymLinkAliasChecker());

        // A WebAppContext is a ContextHandler as well so it needs to be set to
        // the server so it is aware of where to send the appropriate requests.
        server.setHandler(webapp);

        // request logs
        NCSARequestLog requestLog = new NCSARequestLog();
        requestLog.setAppend(true);
        requestLog.setExtended(false);
        requestLog.setLogTimeZone("GMT");
//        requestLog.setLogLatency(true);
//        requestLog.setRetainDays(90);

        server.setRequestLog(requestLog);

        // Start things up!
        server.start();

        // The use of server.join() the will make the current thread join and
        // wait until the server is done executing.
        // See http://docs.oracle.com/javase/7/docs/api/java/lang/Thread.html#join()
        server.join();
    }
}
