/*
 * Copyright 2012-2019 the original author or authors
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

import com.couchbase.client.core.error.CASMismatchException;
import com.couchbase.client.core.error.KeyExistsException;
import com.couchbase.client.core.error.KeyNotFoundException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.ReactiveQueryResult;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.join.N1qlJoinResolver;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.mapping.KeySettings;
import org.springframework.data.couchbase.core.mapping.event.AfterSaveEvent;
import org.springframework.data.couchbase.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.couchbase.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.couchbase.core.mapping.event.CouchbaseMappingEvent;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import org.springframework.data.couchbase.core.query.N1qlJoin;
import org.springframework.data.couchbase.core.support.TemplateUtils;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.util.TypeInformation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

public class CouchbaseTemplateSupport implements ApplicationEventPublisherAware {
    protected ApplicationEventPublisher eventPublisher;

    // extending class is responsible for setting these
    protected KeySettings keySettings;
    protected CouchbaseConverter converter;

    protected MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;

    @Override
    public void setApplicationEventPublisher(final ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    protected <T> Mono<T> doRemove(ReactiveCollection client, T objectToRemove, final PersistTo persistTo, final ReplicateTo replicateTo) {
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

    protected <T> Mono<T> doPersist(ReactiveCollection client, T objectToPersist, PersistType persistType, PersistTo persistTo, ReplicateTo replicateTo) {
        final ConvertingPropertyAccessor<T> accessor = getPropertyAccessor(objectToPersist);
        final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(objectToPersist.getClass());
        final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();
        final Long version = versionProperty != null ? accessor.getProperty(versionProperty, Long.class) : null;
        maybeEmitEvent(new BeforeConvertEvent<T>(objectToPersist));
        final CouchbaseDocument converted = new CouchbaseDocument();
        converter.write(objectToPersist, converted);

        maybeEmitEvent(new BeforeSaveEvent<T>(objectToPersist, converted));
        String generatedId = addCommonPrefixAndSuffix(converted.getId());
        converted.setId(generatedId);

        Mono<MutationResult> result;
        switch (persistType) {

            case SAVE:
                if (version == null) {

                    //No version field - no cas
                    result = client.upsert(converted.getId(), converted.getPayload(),
                            UpsertOptions.upsertOptions().durability(persistTo, replicateTo).expiry(Duration.ofSeconds(converted.getExpiration())));
                } else if (version > 0) {
                    //Updating existing document with cas
                    result = client.replace(converted.getId(), converted.getPayload(),
                            ReplaceOptions.replaceOptions().cas(version).durability(persistTo, replicateTo).expiry(Duration.ofSeconds(converted.getExpiration())));
                } else {
                    //Creating new document
                    result = client.insert(converted.getId(), converted.getPayload(), InsertOptions.insertOptions().durability(persistTo, replicateTo).expiry(Duration.ofSeconds(converted.getExpiration())));
                }
                break;
            case UPDATE:
                if (version == null || version <= 0) {
                    result = client.replace(converted.getId(), converted.getPayload(), ReplaceOptions.replaceOptions().durability(persistTo, replicateTo).expiry(Duration.ofSeconds(converted.getExpiration())));
                } else {
                    result = client.replace(converted.getId(), converted.getPayload(), ReplaceOptions.replaceOptions().cas(version).durability(persistTo, replicateTo).expiry(Duration.ofSeconds(converted.getExpiration())));
                }
                break;
            case INSERT:
            default:
                result = client.insert(converted.getId(), converted.getPayload(), InsertOptions.insertOptions().durability(persistTo, replicateTo).expiry(Duration.ofSeconds(converted.getExpiration())));
                break;
        }
        // now lets write the new version back into the object (if it has @Version annotation)
        return result
                .flatMap(mutationResult -> {
                    CouchbasePersistentProperty idProperty = persistentEntity.getIdProperty();
                    Object entityId = accessor.getProperty(idProperty);
                    if (!generatedId.equals(entityId)) {
                        accessor.setProperty(idProperty, generatedId);
                    }
                    if (versionProperty != null) {
                        accessor.setProperty(versionProperty, mutationResult.cas());
                    }
                    maybeEmitEvent(new AfterSaveEvent<T>(objectToPersist, converted));
                    return Mono.just(objectToPersist);
                }).onErrorResume(e -> {
                    if (e instanceof KeyExistsException) {
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

    protected <T> Mono<T> doFind(ReactiveCollection client, String id, Class<T> entityClass) {
        final CouchbasePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
        if (entity.isTouchOnRead()) {
            Duration duration = Duration.ofSeconds(entity.getExpiry());
            return client.getAndTouch(id, duration)
                    .flatMap(res -> {
                        T doc = res.contentAs(entityClass);
                        return Mono.just(mapToEntity(id, doc, res.cas()));
                    })
                    .onErrorResume(e -> {
                        if (e instanceof KeyNotFoundException) {
                            return Mono.empty();
                        } else {
                            return Mono.error(TemplateUtils.translateError(e));
                        }
                    });
        } else {
            return client.get(id)
                    .flatMap(res -> {
                        T doc = res.contentAs(entityClass);
                        return Mono.just(mapToEntity(id, doc, res.cas()));
                    })
                    .onErrorResume(e -> {
                                if (e instanceof KeyNotFoundException) {
                                    return Mono.empty();
                                } else {
                                    return Mono.error(TemplateUtils.translateError(e));
                                }
                            });
        }
    }

    public Mono<Boolean> doExists(ReactiveCollection client, String id) {
        return client.exists(id).flatMap(result -> Mono.just(result.exists()))
                .doOnError(e -> TemplateUtils.translateError(e));
    }

    protected Mono<ReactiveQueryResult> doQueryN1QL(ReactiveCluster cluster, N1QLQuery query) {
        return cluster.query(query.getExpression(), query.getOptions())
                .doOnError(e -> TemplateUtils.translateError(e));
    }

    protected QueryResult doQueryN1QL(Cluster cluster, N1QLQuery query) {
        return cluster.query(query.getExpression(), query.getOptions());
    }

    protected <T> Flux<T> doFindByN1QL(ReactiveCluster cluster, N1QLQuery query, Class<T> entityClass) {
        // TODO: this use of the JacksonTransformers.MAPPER is probably a hack, and wrong
        return doQueryN1QL(cluster, query)
                .flatMapMany(res -> res.rowsAsObject())
                .flatMap(obj ->  {
                        try {
                            T converted = JacksonTransformers.MAPPER.readValue(obj.toString(), entityClass);
                            return Flux.just(mapToEntity(obj.getString(TemplateUtils.SELECT_ID),
                                    converted,
                                    obj.getLong(TemplateUtils.SELECT_CAS)));
                        } catch (Throwable t) {
                            return Flux.error(new CouchbaseQueryExecutionException("Unable to execute n1ql query", t));
                        }
                })
                .doOnError(throwable -> {
                        if (throwable instanceof CouchbaseQueryExecutionException) {
                            Flux.error(throwable);
                        } else {
                            Flux.error(new CouchbaseQueryExecutionException("Unable to execute n1ql query", throwable));
                        }
                });
    }



    protected final <T> ConvertingPropertyAccessor<T> getPropertyAccessor(T source) {
        CouchbasePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(source.getClass());
        PersistentPropertyAccessor<T> accessor = entity.getPropertyAccessor(source);

        return new ConvertingPropertyAccessor<>(accessor, converter.getConversionService());
    }


    // Convenient to make error messages track better with what was actually called
    protected enum PersistType {
        SAVE("Save", "Upsert"),
        INSERT("Insert", "Insert"),
        UPDATE("Update", "Replace");

        private final String sdkOperationName;
        private final String springDataOperationName;

        PersistType(String sdkOperationName, String springDataOperationName) {
            this.sdkOperationName = sdkOperationName;
            this.springDataOperationName = springDataOperationName;
        }
        public String getSdkOperationName() { return sdkOperationName; }

        public String getSpringDataOperationName() { return springDataOperationName; }
    }

    // TODO: this should eventually be private I believe - should only be called in the
    //       code here, not in the extending Template classes
    protected <T> void maybeEmitEvent(final CouchbaseMappingEvent<T> event) {
         // TODO: fix this!  Events were giving me exceptions - re-enable soon and fix
        if (eventPublisher != null) {
            eventPublisher.publishEvent(event);
        }
    }

    private<T> CouchbasePersistentProperty idProperty(T object) {
        return mappingContext.getRequiredPersistentEntity(object.getClass()).getIdProperty();
    }

    private <T> CouchbasePersistentProperty versionProperty(T object) {
        return mappingContext.getRequiredPersistentEntity(object.getClass()).getVersionProperty();
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

    private String addCommonPrefixAndSuffix(final String id) {
        String convertedKey = id;
        if (this.keySettings == null) {
            return id;
        }
        String prefix = this.keySettings.prefix();
        String delimiter = this.keySettings.delimiter();
        String suffix = this.keySettings.suffix();
        if (prefix != null && !prefix.equals("")) {
            convertedKey = prefix + delimiter + convertedKey;
        }
        if (suffix != null && !suffix.equals("")) {
            convertedKey = convertedKey + delimiter + suffix;
        }
        return convertedKey;
    }
    // TODO: when everything is moved over, this should be private
    protected <T> T mapToEntity(String id, T readEntity, long cas) {

        if (readEntity == null) {
            return null;
        }

        final ConvertingPropertyAccessor<T> accessor = getPropertyAccessor(readEntity);
        CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(readEntity.getClass());

        if (persistentEntity.getVersionProperty() != null) {
            accessor.setProperty(persistentEntity.getVersionProperty(), cas);
        }

        if (persistentEntity.getIdProperty() != null) {
            accessor.setProperty(persistentEntity.getIdProperty(), id);
        }

        persistentEntity.doWithProperties((PropertyHandler<CouchbasePersistentProperty>) prop -> {
            if (prop.isAnnotationPresent(N1qlJoin.class)) {
                N1qlJoin definition = prop.findAnnotation(N1qlJoin.class);
                TypeInformation type =  prop.getTypeInformation().getActualType();
                Class clazz = type.getType();
                N1qlJoinResolver.N1qlJoinResolverParameters parameters = new N1qlJoinResolver.N1qlJoinResolverParameters(definition, id, persistentEntity.getTypeInformation(), type);
                if (N1qlJoinResolver.isLazyJoin(definition)) {
                    // TODO: surely there is another way to get the superclass to pass in here
                    N1qlJoinResolver.N1qlJoinProxy proxy = new N1qlJoinResolver.N1qlJoinProxy((CouchbaseTemplate)this, parameters);
                    accessor.setProperty(prop, java.lang.reflect.Proxy.newProxyInstance(List.class.getClassLoader(),
                            new Class[]{List.class}, proxy));
                } else {
                    // TODO: surely there is another way to get the superclass to pass in here
                    accessor.setProperty(prop, N1qlJoinResolver.doResolve((CouchbaseTemplate)this, parameters, clazz));
                }
            }
        });

        return accessor.getBean();
    }
}
