package io.grakn.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static io.grakn.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.CLUSTER_FILE_PATH;
import static io.grakn.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.DIRECTORY;
import static io.grakn.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.ISOLATION_LEVEL;
import static io.grakn.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.VERSION;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;

/**
 * Experimental FoundationDB storage manager implementation.
 *
 */
public class FoundationDBStoreManager extends AbstractStoreManager implements OrderedKeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBStoreManager.class);

    private final Map<String, FoundationDBKeyValueStore> stores;

    private FDB fdb;
    private Database db;
    private final StoreFeatures features;
    private DirectorySubspace rootDirectory;
    private final String rootDirectoryName;
    private final FoundationDBTransaction.IsolationLevel isolationLevel;

    public FoundationDBStoreManager(Configuration configuration) throws BackendException {
        super(configuration);
        stores = new ConcurrentHashMap<>();

        fdb = FDB.selectAPIVersion(determineFoundationDbVersion(configuration));
        rootDirectoryName = determineRootDirectoryName(configuration);
        db = !"default".equals(configuration.get(CLUSTER_FILE_PATH)) ?
                fdb.open(configuration.get(CLUSTER_FILE_PATH)) : fdb.open();

        final String isolationLevelStr = configuration.get(ISOLATION_LEVEL);
        switch (isolationLevelStr.toLowerCase().trim()) {
            case "serializable":
                isolationLevel = FoundationDBTransaction.IsolationLevel.SERIALIZABLE;
                break;
            case "read_committed_no_write":
                isolationLevel = FoundationDBTransaction.IsolationLevel.READ_COMMITTED_NO_WRITE;
                break;
            case "read_committed_with_write":
                isolationLevel = FoundationDBTransaction.IsolationLevel.READ_COMMITTED_WITH_WRITE;
                break;
            default:
                throw new PermanentBackendException("Unrecognized isolation level " + isolationLevelStr);
        }
        initialize(rootDirectoryName);

        features = new StandardStoreFeatures.Builder()
                .orderedScan(true)
                .transactional(transactional)
                .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
                .locking(true)
                .keyOrdered(true)
                .supportsInterruption(false)
                .optimisticLocking(true)
                .multiQuery(true)
                .build();
    }

    private void initialize(String directoryName) throws BackendException {
        try {
            // create the root directory to hold the JanusGraph data
            rootDirectory = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from(directoryName)).get();
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig txCfg) throws BackendException {
        try {
            final Transaction tx = db.createTransaction();

            final StoreTransaction fdbTx = new FoundationDBTransaction(db, tx, txCfg, isolationLevel);

            if (log.isTraceEnabled()) {
                log.trace("FoundationDB tx created", new TransactionBegin(fdbTx.toString()));
            }

            return fdbTx;
        } catch (Exception e) {
            throw new PermanentBackendException("Could not start FoundationDB transaction", e);
        }
    }

    @Override
    public FoundationDBKeyValueStore openDatabase(String name) throws BackendException {
        Preconditions.checkNotNull(name);
        if (stores.containsKey(name)) {
            return stores.get(name);
        }
        try {
            final DirectorySubspace storeDb = rootDirectory.createOrOpen(db, PathUtil.from(name)).get();
            log.debug("Opened database {}", name, new Throwable());

            FoundationDBKeyValueStore store = new FoundationDBKeyValueStore(name, storeDb, this);
            stores.put(name, store);
            return store;
        } catch (Exception e) {
            throw new PermanentBackendException("Could not open FoundationDB data store", e);
        }
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException {
        for (Map.Entry<String,KVMutation> mutation : mutations.entrySet()) {
            FoundationDBKeyValueStore store = openDatabase(mutation.getKey());
            KVMutation mutationValue = mutation.getValue();

            if (!mutationValue.hasAdditions() && !mutationValue.hasDeletions()) {
                log.debug("Empty mutation set for {}, doing nothing", mutation.getKey());
            } else {
                log.debug("Mutating {}", mutation.getKey());
            }

            if (mutationValue.hasAdditions()) {
                for (KeyValueEntry entry : mutationValue.getAdditions()) {
                    store.insert(entry.getKey(),entry.getValue(),txh);
                    log.trace("Insertion on {}: {}", mutation.getKey(), entry);
                }
            }
            if (mutationValue.hasDeletions()) {
                for (StaticBuffer del : mutationValue.getDeletions()) {
                    store.delete(del,txh);
                    log.trace("Deletion on {}: {}", mutation.getKey(), del);
                }
            }
        }
    }

    void removeDatabase(FoundationDBKeyValueStore db) {
        if (!stores.containsKey(db.getName())) {
            throw new IllegalArgumentException("Tried to remove an unkown database from the storage manager");
        }
        String name = db.getName();
        stores.remove(name);
        log.debug("Removed database {}", name);
    }


    @Override
    public void close() throws BackendException {
        if (fdb != null) {
            if (!stores.isEmpty())
                throw new IllegalStateException("Cannot shutdown manager since some databases are still open");
            try {
                // TODO this looks like a race condition
                //Wait just a little bit before closing so that independent transaction threads can clean up.
                Thread.sleep(30);
            } catch (InterruptedException e) {
                //Ignore
            }
            try {
                db.close();
            } catch (Exception e) {
                throw new PermanentBackendException("Could not close FoundationDB database", e);
            }
        }

    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            rootDirectory.removeIfExists(db).get();
        } catch (Exception e) {
            throw new PermanentBackendException("Could not clear FoundationDB storage", e);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        // @todo
        try {
            return DirectoryLayer.getDefault().exists(db, PathUtil.from(rootDirectoryName)).get();
        } catch (InterruptedException e) {
            throw new PermanentBackendException(e);
        } catch (ExecutionException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }


    private static class TransactionBegin extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionBegin(String msg) {
            super(msg);
        }
    }

    private String determineRootDirectoryName(Configuration config) {
        if (!config.has(DIRECTORY) && (config.has(GRAPH_NAME))) return config.get(GRAPH_NAME);
        return config.get(DIRECTORY);
    }

    private int determineFoundationDbVersion(Configuration config) {
        return config.get(VERSION);
    }
}