package test.edu.upenn.cis.stormlite;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;

/**
 * A trivial bolt that simply outputs its input stream to the
 * console
 *
 * @author zives
 */
public class PrintBolt implements IRichBolt {
    static Logger log = Logger.getLogger(PrintBolt.class);

    Fields myFields = new Fields("key", "value");
    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the PrintBolt, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    private Path outputFile;
    private PrintWriter writer;

    private int eosExpected;

    @Override
    public void cleanup() {

        log.info("PrintBolt Closing the file.");

        writer.close();

    }

    @Override
    public void execute(Tuple input) {

        if (!input.isEndOfStream()) {

            String line = input.getStringByField("key") + "," + input.getStringByField("value");
            writer.println(line);

            log.info("PrintBolt " + executorId + " writing line " + line);
        } else {
            eosExpected--;

            log.info("PrintBolt " + executorId + " received EOS; still expecting " + eosExpected + " EOSs");

            if (eosExpected == 0) {
                log.info("PrintBolt " + executorId + " received all EOS; finishing");
                writer.flush();
                writer.close();
            }
        }
    }

    @Override
    public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {

        // TODO don't hardcode
        eosExpected = 2;

        outputFile = Paths.get(stormConf.get("outputDir"), "output.txt");

        try {
            BufferedWriter bufferedWriter = Files.newBufferedWriter(outputFile, Charset.defaultCharset());
            writer = new PrintWriter(bufferedWriter);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getExecutorId() {
        return executorId;
    }

    @Override
    public void setRouter(StreamRouter router) {
        // Do nothing
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(myFields);
    }

    @Override
    public Fields getSchema() {
        return myFields;
    }

}
