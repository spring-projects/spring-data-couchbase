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

import java.util.Collection;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.view.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

/**
 * RxJavaCouchbaseTemplate implements operations using rxjava1 observables
 * @author Subhashni Balakrishnan
 * @since 3.0
 */
public class RxJavaCouchbaseTemplate implements RxJavaCouchbaseOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(RxJavaCouchbaseTemplate.class);

    private Bucket syncClient;
    private AsyncBucket client;
    private final ClusterInfo clusterInfo;
    private final CouchbaseConverter converter;
    private final TranslationService translationService;
    protected final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;
    private Consistency configuredConsistency = Consistency.DEFAULT_CONSISTENCY;

    private static final WriteResultChecking DEFAULT_WRITE_RESULT_CHECKING = WriteResultChecking.NONE;
    private WriteResultChecking writeResultChecking = DEFAULT_WRITE_RESULT_CHECKING;


    public <T> Observable<T> save(T objectToSave) {
        return doPersist(objectToSave, PersistTo.NONE, ReplicateTo.NONE);
    }

    public <T> Observable<T>  save(Iterable<T> batchToSave) {
        return Observable.from(batchToSave)
                .flatMap(object -> save(object));
    }

    public <T> Observable<T> save(T objectToSave, PersistTo persistTo, ReplicateTo replicateTo) {
        return doPersist(objectToSave, persistTo, replicateTo);
    }

    public <T> Observable<T> save(Iterable<T> batchToSave, PersistTo persistTo, ReplicateTo replicateTo) {
        return Observable.from(batchToSave)
                .flatMap(object -> save(object, persistTo, replicateTo));
    }

    public <T> Observable<T> remove(T objectToRemove) {
        return doRemove(objectToRemove, PersistTo.NONE, ReplicateTo.NONE);
    }

    public <T> Observable<T> remove(Iterable<T> batchToRemove) {
        return Observable.from(batchToRemove)
                .flatMap(object -> remove(object));
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
        CouchbasePersistentEntity<?> entity = mappingContext.getPersistentEntity(source.getClass());
        PersistentPropertyAccessor accessor = entity.getPropertyAccessor(source);

        return new ConvertingPropertyAccessor(accessor, converter.getConversionService());
    }


    private <T> Observable<T> doPersist(T objectToPersist, final PersistTo persistTo, final ReplicateTo replicateTo) {
        ensureNotIterable(objectToPersist);

        final ConvertingPropertyAccessor accessor = getPropertyAccessor(objectToPersist);
        final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(objectToPersist.getClass());
        final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();
        final Long version = versionProperty != null ? accessor.getProperty(versionProperty, Long.class) : null;

        final CouchbaseDocument converted = new CouchbaseDocument();
        converter.write(objectToPersist, converted);
        RawJsonDocument doc = encodeAndWrap(converted, version);
        return client.upsert(doc, persistTo, replicateTo)
                .flatMap(rawJsonDocument -> Observable.just(objectToPersist))
                .doOnError(e -> TemplateUtils.translateError(e));
    }

    private <T> Observable<T> doRemove(T objectToRemove, final PersistTo persistTo, final ReplicateTo replicateTo) {
        ensureNotIterable(objectToRemove);
        if(objectToRemove instanceof String) {
            return client.remove((String) objectToRemove, persistTo, replicateTo)
                    .flatMap(rawJsonDocument -> Observable.just(objectToRemove))
                    .doOnError(e -> TemplateUtils.translateError(e));
        } else {
            final ConvertingPropertyAccessor accessor = getPropertyAccessor(objectToRemove);
            final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(objectToRemove.getClass());
            final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();
            final Long version = versionProperty != null ? accessor.getProperty(versionProperty, Long.class) : null;

            final CouchbaseDocument converted = new CouchbaseDocument();
            converter.write(objectToRemove, converted);
            RawJsonDocument doc = encodeAndWrap(converted, version);
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
        final CouchbasePersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
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
        CouchbasePersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(readEntity.getClass());
        if (persistentEntity.hasVersionProperty()) {
            accessor.setProperty(persistentEntity.getVersionProperty(), data.cas());
        }

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

}
