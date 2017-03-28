/*
 * Copyright 2017 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core;

import static org.springframework.data.couchbase.core.CouchbaseTemplate.ensureNotIterable;

import java.util.Optional;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.view.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.*;
import org.springframework.data.couchbase.core.query.Consistency;
import org.springframework.data.couchbase.core.support.TemplateUtils;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import rx.Observable;
import rx.functions.Action4;
import rx.functions.Func3;
import rx.functions.Func4;

/**
 * RxJavaCouchbaseTemplate implements operations using rxjava1 observables
 * @author Subhashni Balakrishnan
 * @author Mark Paluch
 * @author Alex Derkach
 * @since 3.0
 */
public class RxJavaCouchbaseTemplate implements RxJavaCouchbaseOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(RxJavaCouchbaseTemplate.class);

    private static final WriteResultChecking DEFAULT_WRITE_RESULT_CHECKING = WriteResultChecking.NONE;

    protected final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;

    private Bucket syncClient;
    private AsyncBucket client;
    private final ClusterInfo clusterInfo;
    private final CouchbaseConverter converter;
    private final TranslationService translationService;
    private Consistency configuredConsistency = Consistency.DEFAULT_CONSISTENCY;
    private WriteResultChecking writeResultChecking = DEFAULT_WRITE_RESULT_CHECKING;

    public <T> Observable<T> save(T objectToSave) {
        return save(objectToSave, PersistTo.NONE, ReplicateTo.NONE);
    }

    public <T> Observable<T> save(Iterable<T> batchToSave) {
        return Observable.from(batchToSave)
                .flatMap(this::save);
    }

    public <T> Observable<T> save(T objectToSave, PersistTo persistTo, ReplicateTo replicateTo) {
        return doPersist(objectToSave, PersistType.SAVE, persistTo, replicateTo);
    }

    public <T> Observable<T> save(Iterable<T> batchToSave, PersistTo persistTo, ReplicateTo replicateTo) {
        return Observable.from(batchToSave)
                .flatMap(object -> save(object, persistTo, replicateTo));
    }

    @Override
    public <T> Observable<T> insert(T objectToSave) {
        return insert(objectToSave, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Override
    public <T> Observable<T> insert(Iterable<T> batchToSave) {
        return Observable.from(batchToSave)
                .flatMap(this::insert);
    }

    @Override
    public <T> Observable<T> insert(T objectToSave, PersistTo persistTo, ReplicateTo replicateTo) {
        return doPersist(objectToSave, PersistType.INSERT, persistTo, replicateTo);
    }

    @Override
    public <T> Observable<T> insert(Iterable<T> batchToSave, PersistTo persistTo, ReplicateTo replicateTo) {
        return Observable.from(batchToSave)
                .flatMap(objectToSave -> insert(objectToSave, persistTo, replicateTo));
    }

    @Override
    public <T> Observable<T> update(T objectToSave) {
        return update(objectToSave, PersistTo.NONE, ReplicateTo.NONE);
    }

    @Override
    public <T> Observable<T> update(Iterable<T> batchToSave) {
        return Observable.from(batchToSave)
                .flatMap(this::update);
    }

    @Override
    public <T> Observable<T> update(T objectToSave, PersistTo persistTo, ReplicateTo replicateTo) {
        return doPersist(objectToSave, PersistType.UPDATE, persistTo, replicateTo);
    }

    @Override
    public <T> Observable<T> update(Iterable<T> batchToSave, PersistTo persistTo, ReplicateTo replicateTo) {
        return Observable.from(batchToSave)
                .flatMap(objectToSave -> update(objectToSave, persistTo, replicateTo));
    }

    public <T> Observable<T> remove(T objectToRemove) {
        return doRemove(objectToRemove, PersistTo.NONE, ReplicateTo.NONE);
    }

    public <T> Observable<T> remove(Iterable<T> batchToRemove) {
        return Observable.from(batchToRemove)
                .flatMap(this::remove);
    }

    public <T> Observable<T> remove(T objectToRemove, PersistTo persistTo, ReplicateTo replicateTo) {
        return doRemove(objectToRemove, persistTo, replicateTo);
    }

    public <T> Observable<T> remove(Iterable<T> batchToRemove, PersistTo persistTo, ReplicateTo replicateTo) {
        return Observable.from(batchToRemove)
                .flatMap(object -> remove(object, persistTo, replicateTo));
    }

    public RxJavaCouchbaseTemplate(final ClusterInfo clusterInfo, final Bucket client) {
        this(clusterInfo, client, null, null);
    }

    public RxJavaCouchbaseTemplate(final ClusterInfo clusterInfo, final Bucket client, final TranslationService translationService) {
        this(clusterInfo, client, null, translationService);
    }


    public void setWriteResultChecking(WriteResultChecking writeResultChecking) {
        this.writeResultChecking = writeResultChecking == null ? DEFAULT_WRITE_RESULT_CHECKING : writeResultChecking;
    }

    public RxJavaCouchbaseTemplate(final ClusterInfo clusterInfo, final Bucket client,
                                   final CouchbaseConverter converter,
                                   final TranslationService translationService) {
        this.syncClient = client;
        this.clusterInfo = clusterInfo;
        this.client = client.async();
        this.converter = converter == null ? getDefaultConverter() : converter;
        this.translationService = translationService == null ? getDefaultTranslationService() : translationService;
        this.mappingContext = this.converter.getMappingContext();
    }

    private RawJsonDocument encodeAndWrap(final CouchbaseDocument source, Long version) {
        String encodedContent = translationService.encode(source);
        if (version == null) {
            return RawJsonDocument.create(source.getId(), source.getExpiration(), encodedContent);
        } else {
            return RawJsonDocument.create(source.getId(), source.getExpiration(), encodedContent, version);
        }
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

    private final ConvertingPropertyAccessor getPropertyAccessor(Object source) {
        CouchbasePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(source.getClass());
        PersistentPropertyAccessor accessor = entity.getPropertyAccessor(source);

        return new ConvertingPropertyAccessor(accessor, converter.getConversionService());
    }

    private <T> Observable<T> doPersist(T objectToPersist, PersistType persistType, PersistTo persistTo, ReplicateTo replicateTo) {
        // If version is not set - assumption that document is new, otherwise updating
        Optional<Long> version = getVersion(objectToPersist);
        Func3<RawJsonDocument, PersistTo, ReplicateTo, Observable<RawJsonDocument>> persistFunction;
        switch (persistType) {
            case SAVE:
                if (!version.isPresent()) {
                    //No version field - no cas
                    persistFunction = client::upsert;
                } else if (version.get() > 0) {
                    //Updating existing document with cas
                    persistFunction = client::replace;
                } else {
                    //Creating new document
                    persistFunction = client::insert;
                }
                break;
            case UPDATE:
                persistFunction = client::replace;
                break;
            case INSERT:
            default:
                persistFunction = client::insert;
                break;
        }
        return persistFunction.call(toJsonDocument(objectToPersist), persistTo, replicateTo)
                .flatMap(storedDoc -> {
                    if (storedDoc != null && storedDoc.cas() != 0) {
                        setVersion(objectToPersist, storedDoc.cas());
                    }
                    return Observable.just(objectToPersist);
                })
                .onErrorResumeNext(e -> {
                    if (e instanceof DocumentAlreadyExistsException) {
                        throw new OptimisticLockingFailureException(persistType.springDataOperationName +
                                " document with version value failed: " + version.orElse(null), e);
                    }
                    if (e instanceof CASMismatchException) {
                        throw new OptimisticLockingFailureException(persistType.springDataOperationName +
                                " document with version value failed: " + version.orElse(null), e);
                    }
                    return TemplateUtils.translateError(e);
                });
    }

    private <T> RawJsonDocument toJsonDocument(T object) {
        ensureNotIterable(object);

        final CouchbaseDocument converted = new CouchbaseDocument();
        converter.write(object, converted);
        return encodeAndWrap(converted, getVersion(object).orElse(null));
    }

    private <T> Optional<CouchbasePersistentProperty> versionProperty(T object) {
        final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(object.getClass());
        return persistentEntity.getVersionProperty();
    }

    private <T> Optional<Long> getVersion(T object) {
        final ConvertingPropertyAccessor accessor = getPropertyAccessor(object);
        Optional<CouchbasePersistentProperty> versionProperty = versionProperty(object);
        return versionProperty.flatMap(p -> accessor.getProperty(p, Long.class));
    }

    private <T> void setVersion(T object, long cas) {
        final ConvertingPropertyAccessor accessor = getPropertyAccessor(object);
        versionProperty(object).ifPresent(p -> accessor.setProperty(p, Optional.ofNullable(cas)));
    }

    private <T> Observable<T> doRemove(T objectToRemove, final PersistTo persistTo, final ReplicateTo replicateTo) {
        if(objectToRemove instanceof String) {
            return client.remove((String) objectToRemove, persistTo, replicateTo)
                    .flatMap(rawJsonDocument -> Observable.just(objectToRemove))
                    .doOnError(e -> TemplateUtils.translateError(e));
        } else {
            RawJsonDocument doc = toJsonDocument(objectToRemove);
            return client.remove(doc, persistTo, replicateTo)
                    .flatMap(rawJsonDocument -> Observable.just(objectToRemove))
                    .doOnError(e -> TemplateUtils.translateError(e));
        }
    }


    @Override
    public Observable<Boolean> exists(String id) {
        return client.exists(id)
                        .doOnError(e -> TemplateUtils.translateError(e));
    }

    @Override
    public Observable<AsyncN1qlQueryResult> queryN1QL(N1qlQuery query) {
        return client.query(query)
                        .doOnError(e -> TemplateUtils.translateError(e));
    }

    @Override
    public Observable<AsyncViewResult> queryView(ViewQuery query) {
        return client.query(query)
                        .doOnError(e -> TemplateUtils.translateError(e));
    }

    @Override
    public Observable<AsyncSpatialViewResult> querySpatialView(SpatialViewQuery query){
        return client.query(query)
                .doOnError(e -> TemplateUtils.translateError(e));
    }

    @Override
    public <T> Observable<T> findById(String id, Class<T> entityClass) {
        final CouchbasePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
        if (entity.isTouchOnRead()) {
            return client.getAndTouch(id, entity.getExpiry(), RawJsonDocument.class)
                            .switchIfEmpty(Observable.just(null))
                            .map(doc -> mapToEntity(id, doc, entityClass))
                            .doOnError(e -> TemplateUtils.translateError(e));
        } else {
            return client.get(id, RawJsonDocument.class)
                            .switchIfEmpty(Observable.just(null))
                            .map(doc -> mapToEntity(id, doc, entityClass))
                            .doOnError(e -> TemplateUtils.translateError(e));
        }
    }

    @Override
    public <T>Observable<T> findByView(ViewQuery query, Class<T> entityClass) {
        if (!query.isIncludeDocs() || !query.includeDocsTarget().equals(RawJsonDocument.class)) {
            if (query.isOrderRetained()) {
                query.includeDocsOrdered(RawJsonDocument.class);
            } else {
                query.includeDocs(RawJsonDocument.class);
            }
        }
        //we'll always map the document to the entity, hence reduce never makes sense.
        query.reduce(false);

        return queryView(query)
                .flatMap(asyncViewResult -> asyncViewResult.error()
                        .flatMap(error -> Observable.error(new CouchbaseQueryExecutionException("Unable to execute view query due to error:" + error.toString())))
                        .switchIfEmpty(asyncViewResult.rows()))
                .map(row -> {
                    AsyncViewRow asyncViewRow = (AsyncViewRow) row;
                    return asyncViewRow.document(RawJsonDocument.class)
                            .map(doc ->  mapToEntity(doc.id(), doc, entityClass)).toBlocking().single();
                })
                .doOnError(throwable -> Observable.error(new CouchbaseQueryExecutionException("Unable to execute view query", throwable)));
    }


    @Override
    public <T>Observable<T> findByN1QL(N1qlQuery query, Class<T> entityClass) {
        return queryN1QL(query)
                .flatMap(asyncN1qlQueryResult -> asyncN1qlQueryResult.errors()
                        .flatMap(error -> Observable.error(new CouchbaseQueryExecutionException("Unable to execute n1ql query due to error:" + error.toString())))
                        .switchIfEmpty(asyncN1qlQueryResult.rows()))
                .map(row -> {
                    JsonObject json = ((AsyncN1qlQueryRow)row).value();
                    String id = json.getString(TemplateUtils.SELECT_ID);
                    Long cas = json.getLong(TemplateUtils.SELECT_CAS);
                    if (id == null || cas == null) {
                        throw new CouchbaseQueryExecutionException("Unable to retrieve enough metadata for N1QL to entity mapping, " +
                                "have you selected " + TemplateUtils.SELECT_ID + " and " + TemplateUtils.SELECT_CAS + "?");
                    }
                    json = json.removeKey(TemplateUtils.SELECT_ID).removeKey(TemplateUtils.SELECT_CAS);
                    RawJsonDocument entityDoc = RawJsonDocument.create(id, json.toString(), cas);
                    T decoded = mapToEntity(id, entityDoc, entityClass);
                    return decoded;
                })
                .doOnError(throwable -> Observable.error(new CouchbaseQueryExecutionException("Unable to execute n1ql query", throwable)));
    }

    @Override
    public <T>Observable<T> findBySpatialView(SpatialViewQuery query, Class<T> entityClass) {
        return querySpatialView(query)
                .flatMap(spatialViewResult -> spatialViewResult.error()
                        .flatMap(error -> Observable.error(new CouchbaseQueryExecutionException("Unable to execute spatial view query due to error:" + error.toString())))
                        .switchIfEmpty(spatialViewResult.rows()))
                .map(row -> {
                    AsyncSpatialViewRow asyncSpatialViewRow = (AsyncSpatialViewRow) row;
                    return asyncSpatialViewRow.document(RawJsonDocument.class)
                            .map(doc ->  mapToEntity(doc.id(), doc, entityClass))
                            .toBlocking().single();
                })
                .doOnError(throwable -> Observable.error(new CouchbaseQueryExecutionException("Unable to execute spatial view query", throwable)));
    }

    @Override
    public <T>Observable<T> findByN1QLProjection(N1qlQuery query, Class<T> entityClass) {
        return queryN1QL(query)
                .flatMap(asyncN1qlQueryResult -> asyncN1qlQueryResult.errors()
                        .flatMap(error -> Observable.error(new CouchbaseQueryExecutionException("Unable to execute n1ql query due to error:" + error.toString())))
                        .switchIfEmpty(asyncN1qlQueryResult.rows()))
                .map(row -> {
                    JsonObject json = ((AsyncN1qlQueryRow)row).value();
                    T decoded = translationService.decodeFragment(json.toString(), entityClass);
                    return decoded;
                })
                .doOnError(throwable -> Observable.error(new CouchbaseQueryExecutionException("Unable to execute n1ql query", throwable)));
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


    private <T> T mapToEntity(String id, Document<String> data, Class<T> entityClass) {
        if (data == null) {
            return null;
        }

        final CouchbaseDocument converted = new CouchbaseDocument(id);
        Object readEntity = converter.read(entityClass, (CouchbaseDocument) decodeAndUnwrap(data, converted));

        final ConvertingPropertyAccessor accessor = getPropertyAccessor(readEntity);
        CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(readEntity.getClass());
        persistentEntity.getVersionProperty().ifPresent(p -> accessor.setProperty(p, Optional.ofNullable(data.cas())));

        return (T) readEntity;
    }

    /**
     * Decode a {@link Document Document&lt;String&gt;} containing a JSON string
     * into a {@link CouchbaseStorable}
     */
    private CouchbaseStorable decodeAndUnwrap(final Document<String> source, final CouchbaseStorable target) {
        //TODO at some point the necessity of CouchbaseStorable should be re-evaluated
        return translationService.decode(source.content(), target);
    }

    @Override
    public Bucket getCouchbaseBucket() {
        return this.syncClient;
    }

    @Override
    public ClusterInfo getCouchbaseClusterInfo() {
        return this.clusterInfo;
    }

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
