package edu.upenn.cis455.mapreduce.master;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class WorkersMap extends HashMap<String, Map<String, Object>> {

    private final int ACTIVE_DURATION = 30;
    private final String LAST_ACTIVE = "lastActive";

    Stream<Map<String, Object>> getActiveWorkers() {
        return entrySet().stream().map(Entry::getValue).filter(worker -> {
            Instant lastActive = (Instant) worker.get(LAST_ACTIVE);
            return lastActive.isAfter(Instant.now().minusSeconds(ACTIVE_DURATION));
        });
    }
}
