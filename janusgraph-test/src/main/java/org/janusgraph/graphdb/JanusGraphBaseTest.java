// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphQuery;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.JanusGraphVertexQuery;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.LogManager;
import org.janusgraph.diskstorage.log.kcvs.KCVSLogManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.types.StandardEdgeLabelMaker;
import org.janusgraph.testutil.TestGraphConfigs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.MANAGEMENT_LOG;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TRANSACTION_LOG;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.USER_LOG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public abstract class JanusGraphBaseTest {

    public static final String LABEL_NAME = T.label.getAccessor();
    public static final String ID_NAME = T.id.getAccessor();

    public WriteConfiguration config;
    public BasicConfiguration readConfig;
    public StandardJanusGraph graph;
    public StoreFeatures features;
    public JanusGraphTransaction tx;
    public JanusGraphManagement mgmt;
    public TestInfo testInfo;

    public Map<String, LogManager> logManagers;

    public JanusGraphBaseTest() {
    }

    public abstract WriteConfiguration getConfigurationWithRandomKeyspace();

    public Configuration getConfig() {
        return new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, config.copy(), BasicConfiguration.Restriction.NONE);
    }

    public void clearGraph(WriteConfiguration config) throws BackendException {
        Backend backend = getBackend(config);
        backend.clearStorage();
        backend.close();
    }

    private Backend getBackend(WriteConfiguration config) {
        ModifiableConfiguration adjustedConfig = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, config.copy(), BasicConfiguration.Restriction.NONE);
        adjustedConfig.set(GraphDatabaseConfiguration.LOCK_LOCAL_MEDIATOR_GROUP, "tmp");
        adjustedConfig.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID, "inst");
        KeyColumnValueStoreManager manager = JanusGraphFactory.getStoreManager(adjustedConfig);
        return new Backend(adjustedConfig, manager);
    }

    public static void fancyPrintOut(TestInfo testInfo) {
        // Don't print anything if tests started from within IntelliJ
        // TODO add system property when invoking tests from CircleCI
        if (System.getProperties().containsKey("idea.test.cyclic.buffer.size")) return;
        int totLength = 170;
        String centralText = testInfo.getTestClass().get().getSimpleName() + ": " + testInfo.getDisplayName();
        int rightSpaceLength = totLength - centralText.length();
        StringBuilder rightSpace = new StringBuilder();
        for (int i = 0; i < rightSpaceLength; i++) {
            rightSpace.append("=");
        }
        System.out.println("\n\nRUNNING: [ " + centralText + " ] " + rightSpace.toString());
    }

    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        fancyPrintOut(testInfo);
        this.testInfo = testInfo;
        this.config = getConfigurationWithRandomKeyspace();
        TestGraphConfigs.applyOverrides(config);
        logManagers = new HashMap<>();
        readConfig = new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, config, BasicConfiguration.Restriction.NONE);
        open(config);
    }

    public void open(WriteConfiguration config) {
        long s = System.currentTimeMillis();
        graph = JanusGraphFactory.open(config);
        long e = System.currentTimeMillis();
        System.out.println("Time to open a new Graph: " + (e - s));
        graph.close();
        s = System.currentTimeMillis();
        graph = JanusGraphFactory.open(config);
        e = System.currentTimeMillis();
        System.out.println("Time to open a new Graph: " + (e - s));
        features = graph.getConfiguration().getStoreFeatures();
        tx = graph.newTransaction();
        mgmt = graph.openManagement();
    }

    @AfterEach
    public void tearDown() throws Exception {
        close();
        closeLogs();
        // This is needed because when using Cassandra the memory usage increases with every new keyspace,
        // to avoid killing the Garbage Collector we delete every keyspace once we're done with it.
        JanusGraphFactory.drop(graph);
    }

    public void finishSchema() {
        if (mgmt != null && mgmt.isOpen())
            mgmt.commit();
        mgmt = graph.openManagement();
        newTx();
        graph.tx().commit();
    }

    public void close() {
        if (mgmt != null && mgmt.isOpen()) mgmt.rollback();
        if (null != tx && tx.isOpen())
            tx.commit();


        if (null != graph && graph.isOpen())
            graph.close();
        config.close();
        readConfig.close();
    }

    public void newTx() {
        if (null != tx && tx.isOpen())
            tx.commit();
        tx = graph.newTransaction();
    }

    public static Map<TestConfigOption, Object> validateConfigOptions(Object... settings) {
        //Parse settings
        Preconditions.checkArgument(settings.length % 2 == 0, "Expected even number of settings: %s", settings);
        final Map<TestConfigOption, Object> options = Maps.newHashMap();
        for (int i = 0; i < settings.length; i = i + 2) {
            Preconditions.checkArgument(settings[i] instanceof TestConfigOption, "Expected configuration option but got: %s", settings[i]);
            Preconditions.checkNotNull(settings[i + 1], "Null setting at position [%s]", i + 1);
            options.put((TestConfigOption) settings[i], settings[i + 1]);
        }
        return options;
    }

    public void clopen(Object... settings) {
        if (mgmt != null && mgmt.isOpen()) mgmt.rollback();
        if (null != tx && tx.isOpen()) tx.commit();
        if (settings != null && settings.length > 0) {
            Map<TestConfigOption, Object> options = validateConfigOptions(settings);
            JanusGraphManagement janusGraphManagement = null;
            ModifiableConfiguration modifiableConfiguration = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, config, BasicConfiguration.Restriction.LOCAL);
            for (Map.Entry<TestConfigOption, Object> option : options.entrySet()) {
                if (option.getKey().option.isLocal()) {
                    modifiableConfiguration.set(option.getKey().option, option.getValue(), option.getKey().umbrella);
                } else {
                    if (janusGraphManagement == null) janusGraphManagement = graph.openManagement();
                    janusGraphManagement.set(ConfigElement.getPath(option.getKey().option, option.getKey().umbrella), option.getValue());
                }
            }
            if (janusGraphManagement != null) janusGraphManagement.commit();
            modifiableConfiguration.close();
        }
        if (null != graph && null != graph.tx() && graph.tx().isOpen())
            graph.tx().commit();
        if (null != graph && graph.isOpen())
            graph.close();
        Preconditions.checkNotNull(config);
        open(config);
    }


    public static TestConfigOption option(ConfigOption option, String... umbrella) {
        return new TestConfigOption(option, umbrella);
    }

    public static final class TestConfigOption {

        public final ConfigOption option;
        public final String[] umbrella;

        public TestConfigOption(ConfigOption option, String... umbrella) {
            Preconditions.checkNotNull(option);
            this.option = option;
            if (umbrella == null) umbrella = new String[0];
            this.umbrella = umbrella;
        }
    }

    /*
    ========= Log Helpers ============
     */

    private KeyColumnValueStoreManager logStoreManager = null;

    private void closeLogs() {
        try {
            for (LogManager lm : logManagers.values()) lm.close();
            logManagers.clear();
            if (logStoreManager != null) {
                logStoreManager.close();
                logStoreManager = null;
            }
        } catch (BackendException e) {
            throw new JanusGraphException(e);
        }
    }

    public void closeLogManager(String logManagerName) {
        if (logManagers.containsKey(logManagerName)) {
            try {
                logManagers.remove(logManagerName).close();
            } catch (BackendException e) {
                throw new JanusGraphException("Could not close LOG manager " + logManagerName, e);
            }
        }
    }

    public Log openUserLog(String identifier) {
        return openLog(USER_LOG, GraphDatabaseConfiguration.USER_LOG_PREFIX + identifier);
    }

    public Log openTxLog() {
        return openLog(TRANSACTION_LOG, Backend.SYSTEM_TX_LOG_NAME);
    }

    private Log openLog(String logManagerName, String logName) {
        try {
            final ModifiableConfiguration configuration = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, config.copy(), BasicConfiguration.Restriction.NONE);
            configuration.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID, "reader");
            configuration.set(GraphDatabaseConfiguration.LOG_READ_INTERVAL, Duration.ofMillis(500L), logManagerName);
            if (logStoreManager == null) {
                logStoreManager = JanusGraphFactory.getStoreManager(configuration);
            }
            final StoreFeatures f = logStoreManager.getFeatures();
            final boolean part = f.isDistributed() && f.isKeyOrdered();
            if (part) {
                for (String partitionedLogName : new String[]{USER_LOG, TRANSACTION_LOG, MANAGEMENT_LOG})
                    configuration.set(KCVSLogManager.LOG_MAX_PARTITIONS, 8, partitionedLogName);
            }
            Preconditions.checkNotNull(logStoreManager);
            if (!logManagers.containsKey(logManagerName)) {
                //Open LOG manager - only supports KCVSLog
                Configuration logConfig = configuration.restrictTo(logManagerName);
                logManagers.put(logManagerName, new KCVSLogManager(logStoreManager, logConfig));
            }
            Preconditions.checkState(logManagers.containsKey(logManagerName));
            return logManagers.get(logManagerName).openLog(logName);
        } catch (BackendException e) {
            throw new JanusGraphException("Could not open LOG: " + logName, e);
        }
    }

    /*
    ========= Schema Type Definition Helpers ============
     */

    public PropertyKey makeVertexIndexedKey(String name, Class dataType) {
        PropertyKey key = mgmt.makePropertyKey(name).dataType(dataType).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex(name, Vertex.class).addKey(key).buildCompositeIndex();
        return key;
    }

    public PropertyKey makeVertexIndexedUniqueKey(String name, Class dataType) {
        PropertyKey key = mgmt.makePropertyKey(name).dataType(dataType).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex(name, Vertex.class).addKey(key).unique().buildCompositeIndex();
        return key;
    }

    public void createExternalVertexIndex(PropertyKey key, String backingIndex) {
        createExternalIndex(key, Vertex.class, backingIndex);
    }

    public void createExternalEdgeIndex(PropertyKey key, String backingIndex) {
        createExternalIndex(key, Edge.class, backingIndex);
    }

    public JanusGraphIndex getExternalIndex(Class<? extends Element> clazz, String backingIndex) {
        String prefix;
        if (Vertex.class.isAssignableFrom(clazz)) prefix = "v";
        else if (Edge.class.isAssignableFrom(clazz)) prefix = "e";
        else if (JanusGraphVertexProperty.class.isAssignableFrom(clazz)) prefix = "p";
        else throw new AssertionError(clazz.toString());

        final String indexName = prefix + backingIndex;
        JanusGraphIndex index = mgmt.getGraphIndex(indexName);
        if (index == null) {
            index = mgmt.buildIndex(indexName, clazz).buildMixedIndex(backingIndex);
        }
        return index;
    }

    private void createExternalIndex(PropertyKey key, Class<? extends Element> clazz, String backingIndex) {
        mgmt.addIndexKey(getExternalIndex(clazz, backingIndex), key);
    }

    public PropertyKey makeKey(String name, Class dataType) {
        return mgmt.makePropertyKey(name).dataType(dataType).cardinality(Cardinality.SINGLE).make();
    }

    public EdgeLabel makeLabel(String name) {
        return mgmt.makeEdgeLabel(name).make();
    }

    public EdgeLabel makeKeyedEdgeLabel(String name, PropertyKey sort, PropertyKey signature) {
        return ((StandardEdgeLabelMaker) tx.makeEdgeLabel(name)).sortKey(sort).signature(signature).directed().make();
    }

    /*
    ========= General Helpers ===========
     */

    public static final int DEFAULT_THREAD_COUNT = 4;

    public static int getThreadCount() {
        final String s = System.getProperty("janusgraph.test.threads");
        if (null != s)
            return Integer.valueOf(s);
        else
            return DEFAULT_THREAD_COUNT;
    }

    public static int wrapAround(int value, int maxValue) {
        value = value % maxValue;
        if (value < 0) value = value + maxValue;
        return value;
    }

    public JanusGraphVertex getVertex(String key, Object value) {
        return getVertex(tx, key, value);
    }

    public static JanusGraphVertex getVertex(JanusGraphTransaction tx, String key, Object value) {
        return getOnlyElement(tx.query().has(key, value).vertices(), null);
    }

    public static double round(double d) {
        return Math.round(d * 1000.0) / 1000.0;
    }

    public static JanusGraphVertex getOnlyVertex(JanusGraphQuery<?> query) {
        return getOnlyElement(query.vertices());
    }

    public static JanusGraphEdge getOnlyEdge(JanusGraphVertexQuery<?> query) {
        return getOnlyElement(query.edges());
    }

    public static <E> E getOnlyElement(Iterable<E> traversal) {
        return getOnlyElement(traversal.iterator());
    }

    public static <E> E getOnlyElement(Iterator<E> traversal) {
        if (!traversal.hasNext()) throw new NoSuchElementException();
        return getOnlyElement(traversal, null);
    }

    public static <E> E getOnlyElement(Iterable<E> traversal, E defaultElement) {
        return getOnlyElement(traversal.iterator(), defaultElement);
    }

    public static <E> E getOnlyElement(Iterator<E> traversal, E defaultElement) {
        if (!traversal.hasNext()) return defaultElement;
        E result = traversal.next();
        if (traversal.hasNext())
            throw new IllegalArgumentException("Traversal contains more than 1 element: " + result + ", " + traversal.next());
        return result;
    }

    public static void assertMissing(JanusGraphTransaction g, Object vid) {
        assertFalse(g.vertices(vid).hasNext());
    }

    public static JanusGraphVertex getV(JanusGraphTransaction tx, Object vid) {
        if (!tx.vertices(vid).hasNext()) return null;
        return (JanusGraphVertex) tx.vertices(vid).next();
    }

    public static JanusGraphEdge getE(JanusGraphTransaction g, Object eid) {
        if (!g.edges(eid).hasNext()) return null;
        return (JanusGraphEdge) g.edges(eid).next();
    }

    public static String n(Object obj) {
        if (obj instanceof RelationType) return ((RelationType) obj).name();
        else return obj.toString();
    }

    public static long getId(Element e) {
        return ((JanusGraphElement) e).longId();
    }

    public static void verifyElementOrder(Iterable<? extends Element> elements, String key, Order order, int expectedCount) {
        verifyElementOrder(elements.iterator(), key, order, expectedCount);
    }

    public static void verifyElementOrder(Iterator<? extends Element> elements, String key, Order order, int expectedCount) {
        Comparable previous = null;
        int count = 0;
        while (elements.hasNext()) {
            final Element element = elements.next();
            final Comparable current = element.value(key);
            if (previous != null) {
                final int cmp = previous.compareTo(current);
                assertTrue(order == Order.ASC ? cmp <= 0 : cmp >= 0,
                        previous + " <> " + current + " @ " + count);
            }
            previous = current;
            count++;
        }
        assertEquals(expectedCount, count);
    }

    public static <T> Stream<T> asStream(Iterator<T> source) {
        final Iterable<T> iterable = () -> source;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    public JanusGraph getForceIndexGraph() {
        final ModifiableConfiguration adjustedConfig = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, config, BasicConfiguration.Restriction.NONE);
        adjustedConfig.set(GraphDatabaseConfiguration.FORCE_INDEX_USAGE, true);
        final WriteConfiguration writeConfig = adjustedConfig.getConfiguration();
        TestGraphConfigs.applyOverrides(writeConfig);
        Preconditions.checkNotNull(writeConfig);
        return JanusGraphFactory.open(writeConfig);
    }

}
