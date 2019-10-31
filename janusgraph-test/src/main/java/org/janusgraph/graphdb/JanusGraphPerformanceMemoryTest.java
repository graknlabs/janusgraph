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

import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.TestCategory;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.util.TestTimeAccumulator;
import org.janusgraph.testutil.JUnitBenchmarkProvider;
import org.janusgraph.testutil.MemoryAssess;
import org.junit.Rule;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.rules.TestRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.janusgraph.testutil.JanusGraphAssert.assertCount;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * These tests focus on the in-memory data structures of individual transactions and how they hold up to high memory pressure
 */
@Tag(TestCategory.MEMORY_TESTS)
public abstract class JanusGraphPerformanceMemoryTest extends JanusGraphBaseTest {

    @Rule
    public TestRule benchmark = JUnitBenchmarkProvider.get();

    @Test
    public void testMemoryLeakage() {
        long memoryBaseline = 0;
        SummaryStatistics stats = new SummaryStatistics();
        int numRuns = 25;
        for (int r = 0; r < numRuns; r++) {
            if (r == 1 || r == (numRuns - 1)) {
                memoryBaseline = MemoryAssess.getMemoryUse();
                stats.addValue(memoryBaseline);
                //System.out.println("Memory before run "+(r+1)+": " + memoryBaseline / 1024 + " KB");
            }
            for (int t = 0; t < 1000; t++) {
                graph.addVertex();
                graph.tx().rollback();
                JanusGraphTransaction tx = graph.newTransaction();
                tx.addVertex();
                tx.rollback();
            }
            if (r == 1 || r == (numRuns - 1)) {
                memoryBaseline = MemoryAssess.getMemoryUse();
                stats.addValue(memoryBaseline);
                //System.out.println("Memory after run " + (r + 1) + ": " + memoryBaseline / 1024 + " KB");
            }
            clopen();
        }
        System.out.println("Average: " + stats.getMean() + " Std. Dev: " + stats.getStandardDeviation());
        assertTrue(stats.getStandardDeviation() < stats.getMin());
    }

    @Test
    public void testTransactionalMemory() throws Exception {
        makeVertexIndexedUniqueKey("uid", Long.class);
        makeKey("name", String.class);

        PropertyKey time = makeKey("time", Integer.class);
        mgmt.makeEdgeLabel("friend").signature(time).directed().make();
        finishSchema();

        final Random random = new Random();
        final int rounds = 100;
        final int commitSize = 1500;
        final AtomicInteger uidCounter = new AtomicInteger(0);
        Thread[] writeThreads = new Thread[4];
        long start = System.currentTimeMillis();
        TestTimeAccumulator.reset();
        System.out.println("starting writes");
        for (int t = 0; t < writeThreads.length; t++) {
            writeThreads[t] = new Thread(() -> {
                for (int r = 0; r < rounds; r++) {
                    JanusGraphTransaction tx = graph.newTransaction();
                    JanusGraphVertex previous = null;
                    for (int c = 0; c < commitSize; c++) {
                        JanusGraphVertex v = tx.addVertex();
                        long uid = uidCounter.incrementAndGet();
                        v.property(VertexProperty.Cardinality.single, "uid", uid);
                        v.property(VertexProperty.Cardinality.single, "name", "user" + uid);
                        if (previous != null) {
                            v.addEdge("friend", previous, "time", Math.abs(random.nextInt()));
                        }
                        previous = v;
                    }
                    tx.commit();
                }
            });
            writeThreads[t].start();
        }
        for (Thread writeThread : writeThreads) {
            writeThread.join();
        }
        System.out.println("Write time for " + (rounds * commitSize * writeThreads.length) + " vertices & edges: " + (System.currentTimeMillis() - start));
        System.out.println("Time in driver for write: " + TestTimeAccumulator.getTotalTimeInMs());
        final int maxUID = uidCounter.get();
        final int trials = 1000;
        final String fixedName = "john";
        Thread[] readThreads = new Thread[Runtime.getRuntime().availableProcessors() * 2];
        start = System.currentTimeMillis();
        TestTimeAccumulator.reset();
        for (int t = 0; t < readThreads.length; t++) {
            readThreads[t] = new Thread(() -> {
                JanusGraphTransaction tx = graph.newTransaction();
                long randomUniqueId = random.nextInt(maxUID) + 1;
                getVertex(tx, "uid", randomUniqueId).property(VertexProperty.Cardinality.single, "name", fixedName);
                for (int t1 = 1; t1 <= trials; t1++) {
                    JanusGraphVertex v = getVertex(tx, "uid", random.nextInt(maxUID) + 1);
                    assertCount(2, v.properties());
                    int count = 0;
                    for (Object e : v.query().direction(Direction.BOTH).edges()) {
                        count++;
                        assertTrue(((JanusGraphEdge) e).<Integer>value("time") >= 0);
                    }
                    assertTrue(count <= 2);
                }
                assertEquals(getVertex(tx, "uid", randomUniqueId).value("name"), fixedName);
                tx.commit();
            });
            readThreads[t].start();
        }
        for (Thread readThread : readThreads) {
            readThread.join();
        }
        System.out.println("Read time for " + (trials * readThreads.length) + " vertex lookups: " + (System.currentTimeMillis() - start));
        System.out.println("Time in driver for read: " + TestTimeAccumulator.getTotalTimeInMs());

    }

    @Test
    public void test() throws ExecutionException, InterruptedException {

        String indexProperty = "INDEX";
        String labelProperty = "LABEL_ID";
        PropertyKey property = mgmt.makePropertyKey(indexProperty).dataType(String.class).make();
        PropertyKey property2 = mgmt.makePropertyKey(labelProperty).dataType(Long.class).make();
        mgmt.buildIndex(indexProperty, Vertex.class).addKey(property).buildCompositeIndex();
        mgmt.buildIndex(labelProperty, Vertex.class).addKey(property2).buildCompositeIndex();
        mgmt.commit();

        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        // We need a good amount of parallelism to have a good chance to spot possible issues. Don't use smaller values.
        int numberOfConcurrentTransactions = 8;
        int commitSize = 3000;
        int txs = 20;
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfConcurrentTransactions);

        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfConcurrentTransactions; i++) {
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                long threadId = Thread.currentThread().getId();
                System.out.println("threadId: " + threadId);

                for (int txi = 0; txi < txs; txi++) {
                    try (JanusGraphTransaction tx = graph.newTransaction()) {
                        long idShift = threadId * commitSize * txs;
                        for (int j = 0; j < commitSize; j++) {
                            long id = idShift + j;
                            String index = "index-" + id;
                            JanusGraphVertex attr1 = tx.addVertex("ATTRIBUTE");
                            attr1.property(indexProperty, index);
                            attr1.property(labelProperty, 1L);

                            JanusGraphVertex attr2 = tx.addVertex("ATTRIBUTE");
                            attr2.property(indexProperty, index);
                            attr2.property(labelProperty, 2L);

                            JanusGraphVertex attr3 = tx.addVertex("ATTRIBUTE");
                            attr3.property(indexProperty, index);
                            attr3.property(labelProperty, 3L);
                        }
                        tx.commit();
                    }
                }

                return null;
            }, executorService);
            asyncInsertions.add(asyncInsert);
        }
        CompletableFuture.allOf(asyncInsertions.toArray(new CompletableFuture[]{})).get();

        long multiplicity = numberOfConcurrentTransactions * commitSize * txs;
        long noOfConcepts = multiplicity * 3;
        long totalTime = System.currentTimeMillis() - start;
        System.out.println("Concepts: " + noOfConcepts + " totalTime: " + totalTime + " throughput: " + noOfConcepts * 1000 * 60 / (totalTime));

    }
}


