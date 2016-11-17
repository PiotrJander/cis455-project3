package edu.upenn.cis.stormlite.spout;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Simple word spout, largely derived from
 * https://github.com/apache/storm/tree/master/examples/storm-mongodb-examples
 * but customized to use a file called words.txt.
 * 
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public abstract class FileSpout implements IRichSpout {
	static Logger log = Logger.getLogger(FileSpout.class);

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordSpout, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

    /**
	 * The collector is the destination for tuples; you "emit" tuples there
	 */
	SpoutOutputCollector collector;
	
	/**
	 * This is a simple file reader
	 */
	String filename;
    BufferedReader reader;
	Random r = new Random();
	
	int inx = 0;
	boolean sentEof = false;
	
    public FileSpout() {
    	filename = getFilename();
    }
    
    public abstract String getFilename();


    /**
     * Initializes the instance of the spout (note that there can be multiple
     * objects instantiated)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map<String, String> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        
        try {
            File path = new File(conf.get("inputDir"), this.filename);
            log.info("Starting spout for " + path);
            log.info(getExecutorId() + " opening file reader");

            // If we have a worker index, read appropriate file among xyz.txt.0, xyz.txt.1, etc.
        	if (conf.containsKey("workerIndex"))
                reader = new BufferedReader(new FileReader(path + "." + conf.get("workerIndex")));
            else
                reader = new BufferedReader(new FileReader(path));
        } catch (FileNotFoundException e) {
			// Auto-generated catch block
			e.printStackTrace();
		}
    }

    /**
     * Shut down the spout
     */
    @Override
    public void close() {
    	if (reader != null)
	    	try {
				reader.close();
			} catch (IOException e) {
				// Auto-generated catch block
				e.printStackTrace();
			}
    }

    /**
     * The real work happens here, in incremental fashion.  We process and output
     * the next item(s).  They get fed to the collector, which routes them
     * to targets
     */
    @Override
    public synchronized void nextTuple() {
    	if (reader != null && !sentEof) {
	    	try {
		    	String line = reader.readLine();
		    	if (line != null) {
                    log.info(getExecutorId() + " read from file " + getFilename() + ": " + line);
                    String[] words = line.split("[ \\t\\,.]");
		
		    		for (String word: words) {
                        log.info(getExecutorId() + " emitting " + word);
                        this.collector.emit(new Values<Object>(String.valueOf(inx++), word));
		    		}
		    	} else if (!sentEof) {
		        	log.info(getExecutorId() + " finished file " + getFilename() + " and emitting EOS");
	    	        this.collector.emitEndOfStream();
	    	        sentEof = true;
		    	}
	    	} catch (IOException e) {
	    		e.printStackTrace();
	    	}
    	}
        Thread.yield();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "value"));
    }


	@Override
	public String getExecutorId() {
		
		return executorId;
	}


	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

}
