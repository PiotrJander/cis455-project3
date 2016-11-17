package edu.upenn.cis455.mapreduce.worker;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ReduceBoltStore {

    private static final String dbName = "ReduceBoltStore";
    private static Environment env;
    private static EntityStore store;
    private static EntryAccessor entryAccessor;

    public static void init(File envHome) throws DatabaseException {

        try {
            //noinspection ResultOfMethodCallIgnored
            envHome.mkdirs();
        } catch (Exception e) {
            e.printStackTrace();
        }

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        env = new Environment(envHome, envConfig);

        StoreConfig storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        storeConfig.setTransactional(true);
        store = new EntityStore(env, dbName, storeConfig);

        // empty db
        store.truncateClass(Entry.class);

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

    public static void addEntry(String key, String value) {
        Entry entry = entryAccessor.pi.get(key);
        if (entry == null) {
            entryAccessor.pi.put(new Entry(key, value));
        } else {
            entry.addValue(value);
            entryAccessor.pi.put(entry);
        }
    }

    public static List<String> getValues(String key) {
        Entry entry = entryAccessor.pi.get(key);
        if (entry == null) {
            return null;
        } else {
            return entry.values;
        }
    }

    public static Iterator<Entry> getAllEntries() {
        return entryAccessor.pi.entities().iterator();
    }

    @Entity
    public static class Entry {

        @PrimaryKey
        String key;

        ArrayList<String> values;

        public Entry(String key) {
            this.key = key;
            this.values = new ArrayList<>();
        }

        Entry(String key, String value) {
            this.key = key;
            this.values = new ArrayList<>(Collections.singletonList(value));
        }

        public Entry() {
        }

        void addValue(String v) {
            values.add(v);
        }

        public String getKey() {
            return key;
        }

        public ArrayList<String> getValues() {
            return values;
        }
    }

    private static class EntryAccessor {

        PrimaryIndex<String, Entry> pi;

        EntryAccessor(EntityStore store) throws DatabaseException {
            pi = store.getPrimaryIndex(String.class, Entry.class);
        }
    }
}
