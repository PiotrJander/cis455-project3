package edu.upenn.cis455.mapreduce.worker;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

import java.io.File;

public class ReduceBoltStore {

    private static Environment env;
    private static EntityStore store;
    private static EntryAccessor entryAccessor;

    public static void init(File envHome) throws DatabaseException {

        // TODO do we create the dir?
//        // create the directory if it doesn't exist
//        try {
//            //noinspection ResultOfMethodCallIgnored
//            envHome.mkdirs();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        env = new Environment(envHome, envConfig);

        StoreConfig storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        storeConfig.setTransactional(true);
        store = new EntityStore(env, "ReduceBoltStore", storeConfig);

        entryAccessor = new EntryAccessor(store);
    }

    public static void close() {
        if (store != null) {
            try {
                store.close();
            } catch (DatabaseException dbe) {
                System.err.println("Error closing store: " +
                        dbe.toString());
                System.exit(-1);
            }
        }

        if (env != null) {
            try {
                // Finally, close environment.
                env.close();
            } catch (DatabaseException dbe) {
                System.err.println("Error closing env: " +
                        dbe.toString());
                System.exit(-1);
            }
        }
    }

    private static void addEntry(String key, String value) {
        entryAccessor.pi.put(new Entry(key, value));
    }


    @Entity
    private static class Entry {

        static int pkCounter = 0;
        @PrimaryKey
        int pk;
        @SecondaryKey(relate = Relationship.MANY_TO_ONE)
        String key;
        String value;

        public Entry(String key, String value) {
            this.pk = getPkAndInc();
            this.key = key;
            this.value = value;
        }

        public Entry() {
        }

        static int getPkAndInc() {
            return pkCounter++;
        }
    }

    private static class EntryAccessor {

        PrimaryIndex<Integer, Entry> pi;

        SecondaryIndex<String, Integer, Entry> si;

        EntryAccessor(EntityStore store) throws DatabaseException {
            pi = store.getPrimaryIndex(Integer.class, Entry.class);
            si = store.getSecondaryIndex(pi, String.class, "key");
        }
    }
}
