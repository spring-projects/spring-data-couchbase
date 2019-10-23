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
import com.couchbase.client.core.error.CASMismatchException;
import com.couchbase.client.java.Bucket;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;

import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.query.ReactiveQueryResult;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.convert.join.N1qlJoinResolver;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.*;
import org.springframework.data.couchbase.core.query.Consistency;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import org.springframework.data.couchbase.core.query.N1qlJoin;
import org.springframework.data.couchbase.core.support.TemplateUtils;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.util.TypeInformation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.management.openmbean.KeyAlreadyExistsException;
import java.time.Duration;
import java.util.List;


/**
 * RxJavaCouchbaseTemplate implements operations using rxjava1 observables
 * @author Subhashni Balakrishnan
 * @author Mark Paluch
 * @author Alex Derkach
 * @since 3.0
 */
public class RxJavaCouchbaseTemplate implements RxJavaCouchbaseOperations {

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
        return doPersist(objectToSave, PersistType.SAVE, persistTo, replicateTo);
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
        return doPersist(objectToSave, PersistType.INSERT, persistTo, replicateTo);
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
        return doPersist(objectToSave, PersistType.UPDATE, persistTo, replicateTo);
    }

    @Override
    public <T> Flux<T> update(Iterable<T> batchToSave, PersistTo persistTo, ReplicateTo replicateTo) {
        return Flux.fromIterable(batchToSave)
                .flatMap(objectToSave -> update(objectToSave, persistTo, replicateTo));
    }

    public <T> Mono<T> remove(T objectToRemove) {
        return doRemove(objectToRemove, PersistTo.NONE, ReplicateTo.NONE);
    }

    public <T> Flux<T> remove(Iterable<T> batchToRemove) {
        return Flux.fromIterable(batchToRemove)
                .flatMap(this::remove);
    }

    public <T> Mono<T> remove(T objectToRemove, PersistTo persistTo, ReplicateTo replicateTo) {
        return doRemove(objectToRemove, persistTo, replicateTo);
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

    private final <T> ConvertingPropertyAccessor<T> getPropertyAccessor(T source) {

        CouchbasePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(source.getClass());
        PersistentPropertyAccessor<T> accessor = entity.getPropertyAccessor(source);

        return new ConvertingPropertyAccessor<>(accessor, converter.getConversionService());
    }

    private <T> Mono<T> doPersist(T objectToPersist, PersistType persistType, PersistTo persistTo, ReplicateTo replicateTo) {
        // If version is not set - assumption that document is new, otherwise updating
        Long version = getVersion(objectToPersist);
        String id = getId(objectToPersist);
        Mono<MutationResult> result;
        switch (persistType) {

            case SAVE:
                if (version == null) {

                    //No version field - no cas
                    result = client.upsert(id, objectToPersist, UpsertOptions.upsertOptions().durability(persistTo, replicateTo));
                } else if (version > 0) {
                    //Updating existing document with cas
                    result = client.replace(id, objectToPersist, ReplaceOptions.replaceOptions().durability(persistTo, replicateTo));
                } else {
                    //Creating new document
                    result = client.insert(id, objectToPersist, InsertOptions.insertOptions().durability(persistTo, replicateTo));
                }
                break;
            case UPDATE:
                result = client.replace(id, objectToPersist, ReplaceOptions.replaceOptions().durability(persistTo, replicateTo));
                break;
            case INSERT:
            default:
                result = client.insert(id, objectToPersist, InsertOptions.insertOptions().durability(persistTo, replicateTo));
                break;
        }
        return result
                .flatMap(mutationResult -> {
                    setVersion(objectToPersist, mutationResult.cas());
                    return Mono.just(objectToPersist);
                }).onErrorResume(e -> {
                    if (e instanceof KeyAlreadyExistsException) {
                        throw new OptimisticLockingFailureException(persistType.springDataOperationName +
                                " document with version value failed: " + version, e);
                    }
                    if (e instanceof CASMismatchException) {
                        throw new OptimisticLockingFailureException(persistType.springDataOperationName +
                                " document with version value failed: " + version, e);
                    }
                    return Mono.error(TemplateUtils.translateError(e));
                });
    }

    private <T> CouchbasePersistentProperty versionProperty(T object) {
        return mappingContext.getRequiredPersistentEntity(object.getClass()).getVersionProperty();
    }

    private<T> CouchbasePersistentProperty idProperty(T object) {
        return mappingContext.getRequiredPersistentEntity(object.getClass()).getIdProperty();
    }

    private <T> Long getVersion(T object) {

        CouchbasePersistentProperty versionProperty = versionProperty(object);

        return versionProperty == null //
            ? null // 
            : getPropertyAccessor(object).getProperty(versionProperty, Long.class);
    }

    private <T> T setVersion(T object, long version) {
    	
        CouchbasePersistentProperty versionProperty = versionProperty(object);

        if (versionProperty == null) {
        	return object;
        }
        
        final ConvertingPropertyAccessor<T> accessor = getPropertyAccessor(object);
        accessor.setProperty(versionProperty, version);
        return accessor.getBean();
    }

    private<T> String getId(T object) {
        CouchbasePersistentProperty idProperty = idProperty(object);
        return idProperty == null
                ? null //
                : getPropertyAccessor(object).getProperty(idProperty, String.class);
    }

    private<T> T setId(T object, String id) {

        CouchbasePersistentProperty idProperty = idProperty(object);
        if (idProperty == null) {
            return object;
        }
        final ConvertingPropertyAccessor<T> accessor = getPropertyAccessor(object);
        accessor.setProperty(idProperty, id);
        return accessor.getBean();
    }

    private <T> Mono<T> doRemove(T objectToRemove, final PersistTo persistTo, final ReplicateTo replicateTo) {
        if(objectToRemove instanceof String) {
            return client.remove((String)objectToRemove)
                    .flatMap(result -> Mono.just(objectToRemove))
                    .doOnError(e -> TemplateUtils.translateError(e));
        } else {
            String id = getId(objectToRemove);
            return client.remove(id, RemoveOptions.removeOptions().durability(persistTo, replicateTo))
                    .flatMap(res -> Mono.just(objectToRemove))
                    .doOnError(e -> Mono.error(TemplateUtils.translateError(e)));
        }
    }


    @Override
    public Mono<Boolean> exists(String id) {
        return client.exists(id).flatMap(result -> Mono.just(result.exists()))
                        .doOnError(e -> TemplateUtils.translateError(e));
    }

    @Override
    public Mono<ReactiveQueryResult> queryN1QL(N1QLQuery query) {
        return cluster.reactive().query(query.getExpression(), query.getOptions())
                        .doOnError(e -> TemplateUtils.translateError(e));
    }

     @Override
    public <T> Mono<T> findById(String id, Class<T> entityClass) {
        final CouchbasePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
        if (entity.isTouchOnRead()) {
            // TODO: get expiry correct
            Duration duration = Duration.ofSeconds(entity.getExpiry());
            return client.getAndTouch(id, duration)
                    .flatMap(res -> {
                        T doc = res.contentAs(entityClass);
                        return Mono.just(mapToEntity(id, doc, res.cas()));
                    })
                    .switchIfEmpty(Mono.just(null))
                    .doOnError(e -> TemplateUtils.translateError(e));
        } else {
            return client.get(id)
                    .flatMap(res -> {
                        T doc = res.contentAs(entityClass);
                        return Mono.just(mapToEntity(id, doc, res.cas()));
                    })
                    .switchIfEmpty(Mono.just(null))
                    .doOnError(e -> Mono.error(TemplateUtils.translateError(e)));
        }
    }

    @Override
    public <T> Flux<T> findByN1QL(N1QLQuery query, Class<T> entityClass) {
        return queryN1QL(query).flatMapMany(res -> {
                        return res.rowsAs(entityClass).zipWith(res.rowsAsObject())
                                .flatMap(tuple -> {
                                    String id = tuple.getT2().getString(TemplateUtils.SELECT_ID);
                                    long cas = tuple.getT2().getLong(TemplateUtils.SELECT_CAS);
                                    return Flux.just(mapToEntity(id, tuple.getT1(), cas));
                                });
                    }).doOnError(throwable -> {
            Flux.error(new CouchbaseQueryExecutionException("Unable to execute n1ql query", throwable));
        });
    }

    @Override
    public <T>Flux<T> findByN1QLProjection(N1QLQuery query, Class<T> entityClass) {
        return queryN1QL(query)
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


    private <T> T mapToEntity(String id, T readEntity, long cas) {

        if (readEntity == null) {
            return null;
        }

        final ConvertingPropertyAccessor<T> accessor = getPropertyAccessor(readEntity);
        CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(readEntity.getClass());

        if (persistentEntity.getVersionProperty() != null) {
            accessor.setProperty(persistentEntity.getVersionProperty(), cas);
        }

        persistentEntity.doWithProperties((PropertyHandler<CouchbasePersistentProperty>) prop -> {
            /* TODO: need a N1QLJoinResolver that works with RxJavaCouchbaseTemplate.  Commenting
                out for now

                if (prop.isAnnotationPresent(N1qlJoin.class)) {
                N1qlJoin definition = prop.findAnnotation(N1qlJoin.class);
                TypeInformation type =  prop.getTypeInformation().getActualType();
                Class clazz = type.getType();
                N1qlJoinResolver.N1qlJoinResolverParameters parameters = new N1qlJoinResolver.N1qlJoinResolverParameters(definition, id, persistentEntity.getTypeInformation(), type);
                if (N1qlJoinResolver.isLazyJoin(definition)) {
                    N1qlJoinResolver.N1qlJoinProxy proxy = new N1qlJoinResolver.N1qlJoinProxy(this, parameters);
                    accessor.setProperty(prop, java.lang.reflect.Proxy.newProxyInstance(List.class.getClassLoader(),
                            new Class[]{List.class}, proxy));
                } else {
                    accessor.setProperty(prop, N1qlJoinResolver.doResolve(this, parameters, clazz));
                }
            }*/
        });

        return accessor.getBean();
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

    private enum PersistType {
        SAVE("Save", "Upsert"),
        INSERT("Insert", "Insert"),
        UPDATE("Update", "Replace");

        private final String sdkOperationName;
        private final String springDataOperationName;

        PersistType(String sdkOperationName, String springDataOperationName) {
            this.sdkOperationName = sdkOperationName;
            this.springDataOperationName = springDataOperationName;
        }

    }

}
