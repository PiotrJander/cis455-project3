package test.edu.upenn.cis.stormlite;

import edu.upenn.cis455.mapreduce.worker.ReduceBoltStore;
import junit.framework.TestCase;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ReduceBoltStoreTest extends TestCase {

    public void testDatabase() throws Exception {

        ReduceBoltStore.init(new File("store/reduce"));

        ReduceBoltStore.addEntry("foo", "1");
        ReduceBoltStore.addEntry("bar", "1");
        ReduceBoltStore.addEntry("foo", "1");

        List<String> foo = ReduceBoltStore.getValues("foo");
        List<String> bar = ReduceBoltStore.getValues("bar");
        assertEquals(Arrays.asList("1", "1"), foo);
        assertEquals(Collections.singletonList("1"), bar);

        ReduceBoltStore.close();
    }
}
