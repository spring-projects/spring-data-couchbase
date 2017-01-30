/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.repository.support;

import java.io.Serializable;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.AsyncViewRow;
import com.couchbase.client.java.view.ViewQuery;

import org.reactivestreams.Publisher;
import org.springframework.data.couchbase.core.RxJavaCouchbaseOperations;
import org.springframework.data.couchbase.core.query.View;
import org.springframework.data.couchbase.repository.ReactiveCouchbaseRepository;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.repository.util.ReactiveWrapperConverters;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import rx.Single;
import rx.Observable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive repository base implementation for Couchbase.
 *
 * @author Subhashni Balakrishnan
 * @since 3.0
 */
public class SimpleReactiveCouchbaseRepository<T, ID extends Serializable> implements ReactiveCouchbaseRepository<T, ID> {

    /**
     * Holds the reference to the {@link org.springframework.data.couchbase.core.RxJavaCouchbaseTemplate}.
     */
    private final RxJavaCouchbaseOperations operations;

    /**
     * Contains information about the entity being used in this repository.
     */
    private final CouchbaseEntityInformation<T, String> entityInformation;

    /**
     * Custom ViewMetadataProvider.
     */
    private ViewMetadataProvider viewMetadataProvider;

    /**
     * Create a new Repository.
     *
     * @param metadata the Metadata for the entity.
     * @param operations the reference to the reactive template used.
     */
    public SimpleReactiveCouchbaseRepository(final CouchbaseEntityInformation<T, String> metadata,
                                             final RxJavaCouchbaseOperations operations) {
        Assert.notNull(operations);
        Assert.notNull(metadata);

        this.entityInformation = metadata;
        this.operations = operations;
    }

    /**
     * Configures a custom {@link ViewMetadataProvider} to be used to detect {@link View}s to be applied to queries.
     *
     * @param viewMetadataProvider that is used to lookup any annotated View on a query method.
     */
    public void setViewMetadataProvider(final ViewMetadataProvider viewMetadataProvider) {
        this.viewMetadataProvider = viewMetadataProvider;
    }

    protected Mono mapMono(Single single) {
        return ReactiveWrapperConverters.toWrapper(single , Mono.class);
    }

    protected Flux mapFlux(Observable observable) {
        return ReactiveWrapperConverters.toWrapper(observable, Flux.class);
    }

    @SuppressWarnings("unchecked")
    public <S extends T> Mono<S> save(S entity) {
        Assert.notNull(entity, "Entity must not be null!");
        return mapMono(operations.save(entity).toSingle());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S extends T> Flux<S> save(Iterable<S> entities) {
        Assert.notNull(entities, "The given Iterable of entities must not be null!");
        return mapFlux(operations.save(entities));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S extends T> Flux<S> save(Publisher<S> entityStream) {
        Assert.notNull(entityStream, "The given Iterable of entities must not be null!");
        return Flux.from(entityStream)
                .flatMap(object -> save(object));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<T> findOne(ID id) {
        Assert.notNull(id, "The given id must not be null!");
        return mapMono(operations.findById(id.toString(), entityInformation.getJavaType()).toSingle())
                .otherwise(throwable -> {
                    //reactive streams adapter doesn't work with null
                    if(throwable instanceof NullPointerException) {
                        return Mono.empty();
                    }
                    return Mono.just(throwable);
                });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<T> findOne(Mono<ID> mono) {
        Assert.notNull(mono, "The given mono must not be null!");
        return mono.then(
                id -> findOne(id));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Boolean> exists(ID id) {
        Assert.notNull(id, "The given id must not be null!");
        return mapMono(operations.exists(id.toString()).toSingle());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Boolean> exists(Mono<ID> mono) {
        return mono.then(
                id -> exists(id));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Flux<T> findAll() {
        final ResolvedView resolvedView = determineView();
        ViewQuery query = ViewQuery.from(resolvedView.getDesignDocument(), resolvedView.getViewName());
        query.reduce(false);
        query.stale(operations.getDefaultConsistency().viewConsistency());
        return mapFlux(operations.findByView(query, entityInformation.getJavaType()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Flux<T> findAll(final Iterable<ID> ids) {
        final ResolvedView resolvedView = determineView();
        ViewQuery query = ViewQuery.from(resolvedView.getDesignDocument(), resolvedView.getViewName());
        query.reduce(false);
        query.stale(operations.getDefaultConsistency().viewConsistency());
        JsonArray keys = JsonArray.create();
        for (ID id : ids) {
            keys.add(id);
        }
        query.keys(keys);
        return mapFlux(operations.findByView(query, entityInformation.getJavaType()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Flux<T> findAll(Publisher<ID> entityStream) {
        Assert.notNull(entityStream, "The given entityStream must not be null!");
        return Flux.from(entityStream)
                .flatMap(entity -> findOne(entity));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Void> delete(ID id) {
        Assert.notNull(id, "The given id must not be null!");
        return mapMono(operations.remove(id.toString()).map(res -> Observable.<Void>empty()).toSingle());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Void>  delete(T entity) {
        Assert.notNull(entity, "The given id must not be null!");
        return mapMono(operations.remove(entity).map(res -> Observable.<Void>empty()).toSingle());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Void> delete(Iterable<? extends T> entities) {
        Assert.notNull(entities, "The given Iterable of entities must not be null!");
        return mapMono(operations
                .remove(entities)
                .last()
                .map(res -> Observable.<Void>empty()).toSingle());
    }


    @Override
    public Mono<Void> delete(Publisher<? extends T> entityStream) {
        Assert.notNull(entityStream, "The given publisher of entities must not be null!");
        return Flux.from(entityStream)
                .flatMap(entity -> delete(entity)).single();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Long> count() {
        final ResolvedView resolvedView = determineView();
        ViewQuery query = ViewQuery.from(resolvedView.getDesignDocument(), resolvedView.getViewName());
        query.reduce(true);
        query.stale(operations.getDefaultConsistency().viewConsistency());

        return mapMono(operations
                .queryView(query)
                .flatMap(AsyncViewResult::rows)
                .map(asyncViewRow ->
                        Long.valueOf(asyncViewRow.value().toString())).toSingle());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Void> deleteAll() {
        final ResolvedView resolvedView = determineView();
        ViewQuery query = ViewQuery.from(resolvedView.getDesignDocument(), resolvedView.getViewName());
        query.reduce(false);
        query.stale(operations.getDefaultConsistency().viewConsistency());


        return mapMono(operations.queryView(query)
                .map(AsyncViewResult::rows)
                .flatMap(row -> {
                    AsyncViewRow asyncViewRow = (AsyncViewRow) row;
                    return operations.remove(asyncViewRow.id())
                            .onErrorResumeNext(throwable -> {
                                if (throwable instanceof DocumentDoesNotExistException) {
                                    return Observable.empty();
                                }
                                return Observable.error(throwable);
                            });
                })
                .toList()
                .map(list -> Observable.<Void>empty())
                .toSingle());
    }

    /**
     * Returns the information for the underlying template.
     *
     * @return the underlying entity information.
     */
    protected CouchbaseEntityInformation<T, String> getEntityInformation() {
        return entityInformation;
    }

    /**
     * Resolve a View based upon:
     * <p/>
     * 1. Any @View annotation that is present
     * 2. If none are found, default designDocument to be the entity name (lowercase) and viewName to be "all".
     *
     * @return ResolvedView containing the designDocument and viewName.
     */
    private ResolvedView determineView() {
        String designDocument = StringUtils.uncapitalize(entityInformation.getJavaType().getSimpleName());
        String viewName = "all";

        final View view = viewMetadataProvider.getView();

        if (view != null) {
            designDocument = view.designDocument();
            viewName = view.viewName();
        }

        return new ResolvedView(designDocument, viewName);
    }

    @Override
    public RxJavaCouchbaseOperations getCouchbaseOperations(){
        return operations;
    }

    /**
     * Simple holder to allow an easier exchange of information.
     */
    private final class ResolvedView {

        private final String designDocument;
        private final String viewName;

        public ResolvedView(final String designDocument, final String viewName) {
            this.designDocument = designDocument;
            this.viewName = viewName;
        }

        private String getDesignDocument() {
            return designDocument;
        }

        private String getViewName() {
            return viewName;
        }
    }

}
