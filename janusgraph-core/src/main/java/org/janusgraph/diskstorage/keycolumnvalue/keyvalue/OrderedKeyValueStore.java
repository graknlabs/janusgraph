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

package org.janusgraph.diskstorage.keycolumnvalue.keyvalue;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.RecordIterator;

import java.util.List;
import java.util.Map;

/**
 * A {@link KeyValueStore} where the keys are ordered such that keys can be retrieved in order.
 */
public interface OrderedKeyValueStore extends KeyValueStore {

    /**
     * Inserts the given key-value pair into the store. If the key already exists, its value is overwritten by the given one.
     */
    void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws BackendException;

    /**
     * Returns a list of all Key-value pairs ({@link KeyValueEntry} where the keys satisfy the given {@link KVQuery}.
     * That means, the key lies between the query's start and end buffers, satisfied the filter condition (if any) and the position
     * of the result in the result list iterator is less than the given limit.
     * <p>
     * The operation is executed inside the context of the given transaction.
     */
    RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException;


    /**
     * Like {@link #getSlice(KVQuery, org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction)} but executes
     * all of the given queries at once and returns a map of all the result sets of each query.
     * <p>
     * Only supported when the given store implementation supports multi-query, i.e.
     * {@link org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures#hasMultiQuery()} return true. Otherwise
     * this method may throw a {@link UnsupportedOperationException}.
     */
    Map<KVQuery, RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh) throws BackendException;

}
