/*
 * Copyright 2017-2019 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.core;

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.java.Bucket;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.query.ReactiveQueryResult;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.query.Consistency;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Subhashni Balakrishnan
 * @author Alex Derkach
 * @since 3.0
 */
public interface RxJavaCouchbaseOperations {

    <T> Mono<T> save(T objectToSave);

    <T> Flux<T> save(Iterable<T> batchToSave);

    <T> Mono<T> save(T objectToSave, PersistTo persistTo, ReplicateTo replicateTo);

    <T> Flux<T> save(Iterable<T> batchToSave, PersistTo persistTo, ReplicateTo replicateTo);

    <T> Mono<T> insert(T objectToSave);

    <T> Flux<T> insert(Iterable<T> batchToSave);

    <T> Mono<T> insert(T objectToSave, PersistTo persistTo, ReplicateTo replicateTo);

    <T> Flux<T> insert(Iterable<T> batchToSave, PersistTo persistTo, ReplicateTo replicateTo);

    <T> Mono<T> update(T objectToSave);

    <T> Flux<T> update(Iterable<T> batchToSave);

    <T> Mono<T> update(T objectToSave, PersistTo persistTo, ReplicateTo replicateTo);

    <T> Flux<T> update(Iterable<T> batchToSave, PersistTo persistTo, ReplicateTo replicateTo);

    <T> Mono<T> remove(T objectToRemove);

    <T> Mono<T> remove(T objectToRemove, PersistTo persistTo, ReplicateTo replicateTo);

    <T> Flux<T> remove(Iterable<T> batchToRemove);

    <T> Flux<T> remove(Iterable<T> batchToRemove, PersistTo persistTo, ReplicateTo replicateTo);

    Mono<Boolean> exists(String id);

    <T> Mono<T> findById(String id, Class<T> entityClass);

    Mono<ReactiveQueryResult> queryN1QL(N1QLQuery n1ql);

    <T> Flux<T> findByN1QL(N1QLQuery n1ql, Class<T> entityClass);

    <T> Flux<T> findByN1QLProjection(N1QLQuery n1ql, Class<T> fragmentClass);

    Consistency getDefaultConsistency();

    /**
     * Returns the linked {@link Bucket} to this template.
     *
     * @return the client used for the template.
     */
    Bucket getCouchbaseBucket();

    CouchbaseConverter getConverter();

    /**
     * Returns the {@link ClusterConfig} about the cluster linked to this template.
     *
     * @return the info about the cluster the template connects to.
     */
    ClusterConfig getCouchbaseClusterConfig();

    Cluster getCouchbaseCluster();

}
