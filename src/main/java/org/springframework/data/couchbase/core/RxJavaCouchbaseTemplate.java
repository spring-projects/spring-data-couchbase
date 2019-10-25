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
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.query.ReactiveQueryResult;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.*;
import org.springframework.data.couchbase.core.query.Consistency;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import org.springframework.data.mapping.context.MappingContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * RxJavaCouchbaseTemplate implements operations using rxjava1 observables
 * @author Subhashni Balakrishnan
 * @author Mark Paluch
 * @author Alex Derkach
 * @since 3.0
 */
public class RxJavaCouchbaseTemplate extends CouchbaseTemplateSupport implements RxJavaCouchbaseOperations {

    private static final WriteResultChecking DEFAULT_WRITE_RESULT_CHECKING = WriteResultChecking.NONE;

    protected final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;

    private Collection syncClient;
    private ReactiveCollection client;
    private Cluster cluster;
    private final CouchbaseConverter converter;
    private final TranslationService translationService;
    private Consistency configuredConsistency = Consistency.DEFAULT_CONSISTENCY;
    private WriteResultChecking writeResultChecking = DEFAULT_WRITE_RESULT_CHECKING;

    public <T> Mono<T> save(T objectToSave) {
        return save(objectToSave, PersistTo.NONE, ReplicateTo.NONE);
    }

    public <T> Flux<T> save(Iterable<T> batchToSave) {
        return Flux.fromIterable(batchToSave)
                .flatMap(this::save);
    }

    public <T> Mono<T> save(T objectToSave, PersistTo persistTo, ReplicateTo replicateTo) {
        return doPersist(client, objectToSave, PersistType.SAVE, persistTo, replicateTo);
    }

    public <T> Flux<T> save(Iterable<T> batchToSave, PersistTo persistTo, ReplicateTo replicateTo) {
        return Flux.fromIterable(batchToSave)
                .flatMap(object -> save(object, persistTo, replicateTo));
    }

    @Override
    public <T> Mono<T> insert(T objectToSave) {
        return insert(objectToSave, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Override
    public <T> Flux<T> insert(Iterable<T> batchToSave) {
        return Flux.fromIterable(batchToSave)
                .flatMap(this::insert);
    }

    @Override
    public <T> Mono<T> insert(T objectToSave, PersistTo persistTo, ReplicateTo replicateTo) {
        return doPersist(client, objectToSave, PersistType.INSERT, persistTo, replicateTo);
    }

    @Override
    public <T> Flux<T> insert(Iterable<T> batchToSave, PersistTo persistTo, ReplicateTo replicateTo) {
        return Flux.fromIterable(batchToSave)
                .flatMap(objectToSave -> insert(objectToSave, persistTo, replicateTo));
    }

    @Override
    public <T> Mono<T> update(T objectToSave) {
        return update(objectToSave, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Override
    public <T> Flux<T> update(Iterable<T> batchToSave) {
        return Flux.fromIterable(batchToSave)
                .flatMap(this::update);
    }

    @Override
    public <T> Mono<T> update(T objectToSave, PersistTo persistTo, ReplicateTo replicateTo) {
        return doPersist(client, objectToSave, PersistType.UPDATE, persistTo, replicateTo);
    }

    @Override
    public <T> Flux<T> update(Iterable<T> batchToSave, PersistTo persistTo, ReplicateTo replicateTo) {
        return Flux.fromIterable(batchToSave)
                .flatMap(objectToSave -> update(objectToSave, persistTo, replicateTo));
    }

    public <T> Mono<T> remove(T objectToRemove) {
        return doRemove(client, objectToRemove, PersistTo.NONE, ReplicateTo.NONE);
    }

    public <T> Flux<T> remove(Iterable<T> batchToRemove) {
        return Flux.fromIterable(batchToRemove)
                .flatMap(this::remove);
    }

    public <T> Mono<T> remove(T objectToRemove, PersistTo persistTo, ReplicateTo replicateTo) {
        return doRemove(client, objectToRemove, persistTo, replicateTo);
    }

    public <T> Flux<T> remove(Iterable<T> batchToRemove, PersistTo persistTo, ReplicateTo replicateTo) {
        return Flux.fromIterable(batchToRemove)
                .flatMap(object -> remove(object, persistTo, replicateTo));
    }

    public RxJavaCouchbaseTemplate(final Cluster cluster, final Collection client) {
        this(cluster, client, null, null);
    }

    public RxJavaCouchbaseTemplate(final Cluster cluster, final Collection client, final TranslationService translationService) {
        this(cluster, client, null, translationService);
    }


    public void setWriteResultChecking(WriteResultChecking writeResultChecking) {
        this.writeResultChecking = writeResultChecking == null ? DEFAULT_WRITE_RESULT_CHECKING : writeResultChecking;
    }

    public RxJavaCouchbaseTemplate(final Cluster cluster, final Collection client,
                                   final CouchbaseConverter converter,
                                   final TranslationService translationService) {
        this.syncClient = client;
        this.cluster = cluster;
        this.client = client.reactive();
        this.converter = converter == null ? getDefaultConverter() : converter;
        this.translationService = translationService == null ? getDefaultTranslationService() : translationService;
        this.mappingContext = this.converter.getMappingContext();
    }

    private TranslationService getDefaultTranslationService() {
        JacksonTranslationService t = new JacksonTranslationService();
        t.afterPropertiesSet();
        return t;
    }


    private CouchbaseConverter getDefaultConverter() {
        MappingCouchbaseConverter c = new MappingCouchbaseConverter(new CouchbaseMappingContext());
        c.afterPropertiesSet();
        return c;
    }

    public Mono<Boolean> exists(String id) {
        return doExists(client, id);
    }

    @Override
    public Mono<ReactiveQueryResult> queryN1QL(N1QLQuery query) {
        return doQueryN1QL(cluster.reactive(), query);
    }

    @Override
    public <T> Mono<T> findById(String id, Class<T> entityClass) {
        return doFind(client, id, entityClass);
    }

    @Override
    public <T> Flux<T> findByN1QL(N1QLQuery query, Class<T> entityClass) {
        return doFindByN1QL(cluster.reactive(), query, entityClass);
    }

    @Override
    public <T>Flux<T> findByN1QLProjection(N1QLQuery query, Class<T> entityClass) {
        return doQueryN1QL(cluster.reactive(), query)
                .flatMapMany(res ->  Flux.from(res.rowsAs(entityClass)))
                .doOnError(throwable -> Flux.error(new CouchbaseQueryExecutionException("Unable to execute n1ql query", throwable)));
    }


    @Override
    public Consistency getDefaultConsistency() {
        return configuredConsistency;
    }


    public void setDefaultConsistency(Consistency consistency) {
        this.configuredConsistency = consistency;
    }

    @Override
    public CouchbaseConverter getConverter() {
        return this.converter;
    }

    @Override
    public Bucket getCouchbaseBucket() {
        return cluster.bucket(client.bucketName());
    }

    @Override
    public ClusterConfig getCouchbaseClusterConfig() {
        return cluster.core().clusterConfig();
    }

    @Override
    public Cluster getCouchbaseCluster() { return cluster; }


}
