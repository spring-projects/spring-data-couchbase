/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.repository.support;

import java.io.Serializable;


import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import org.reactivestreams.Publisher;
import org.springframework.data.couchbase.core.RxJavaCouchbaseOperations;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import org.springframework.data.couchbase.core.query.View;
import org.springframework.data.couchbase.repository.ReactiveCouchbaseRepository;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.repository.util.ReactiveWrapperConverters;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.springframework.data.couchbase.core.query.N1QLExpression.*;

/**
 * Reactive repository base implementation for Couchbase.
 *
 * @author Subhashni Balakrishnan
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author David Kelly
 * @author Douglas Six
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
        Assert.notNull(operations, "RxJavaCouchbaseOperations must not be null!");
        Assert.notNull(metadata, "CouchbaseEntityInformation must not be null!");

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

    protected Mono mapMono(Mono single) {
        return ReactiveWrapperConverters.toWrapper(single , Mono.class);
    }

    protected Flux mapFlux(Flux observable) {
        return ReactiveWrapperConverters.toWrapper(observable, Flux.class);
    }

    @SuppressWarnings("unchecked")
    public <S extends T> Mono<S> save(S entity) {
        Assert.notNull(entity, "Entity must not be null!");
        return operations.save(entity);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S extends T> Flux<S> saveAll(Iterable<S> entities) {
        Assert.notNull(entities, "The given Iterable of entities must not be null!");
        return operations.save(entities);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S extends T> Flux<S> saveAll(Publisher<S> entityStream) {
        Assert.notNull(entityStream, "The given Iterable of entities must not be null!");
        return Flux.from(entityStream)
                .flatMap(object -> save(object));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<T> findById(ID id) {
        Assert.notNull(id, "The given id must not be null!");
        return operations.findById(id.toString(), entityInformation.getJavaType())
                .onErrorResume(throwable -> {
                    //reactive streams adapter doesn't work with null
                    if(throwable instanceof NullPointerException) {
                        return Mono.empty();
                    }
                    return Mono.error(throwable);
                });
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<T> findById(Publisher<ID> publisher) {
        Assert.notNull(publisher, "The given Publisher must not be null!");
        return Mono.from(publisher).flatMap(
                this::findById);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Boolean> existsById(ID id) {
        Assert.notNull(id, "The given id must not be null!");
        return operations.exists(id.toString());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Boolean> existsById(Publisher<ID> publisher) {
        Assert.notNull(publisher, "The given Publisher must not be null!");
        return Mono.from(publisher).flatMap(
                this::existsById);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Flux<T> findAll() {
        // TODO: figure out a cleaner way
        N1QLExpression expression = select(x("*"))
                .from(i(operations.getCouchbaseBucket().name()))
                .where(x("_class").eq(s(entityInformation.getJavaType().getCanonicalName())));
        QueryScanConsistency consisistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
        N1QLQuery query = new N1QLQuery(expression, QueryOptions.queryOptions().scanConsistency(consisistency));
        return operations.findByN1QL(query, entityInformation.getJavaType());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Flux<T> findAllById(final Iterable<ID> ids) {
        // TODO: figure out a cleaner way
        N1QLExpression expression = select(x("*"))
                .from(i(operations.getCouchbaseBucket().name()))
                .keys(ids);
        QueryScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
        N1QLQuery query = new N1QLQuery(expression, QueryOptions.queryOptions().scanConsistency(consistency));
        return operations.findByN1QL(query, entityInformation.getJavaType());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Flux<T> findAllById(Publisher<ID> entityStream) {
        Assert.notNull(entityStream, "The given entityStream must not be null!");
        return Flux.from(entityStream)
                .flatMap(this::findById);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Void> deleteById(ID id) {
        // TODO: why does this not match the operations (Mono<T> vs Mono<Void>)
        Assert.notNull(id, "The given id must not be null!");
        return operations.remove(id.toString()).flatMap(res-> Mono.empty());
    }

    @Override
    public Mono<Void> deleteById(Publisher<ID> publisher) {
        // TODO: why does this not match the operations (Mono<T> vs Mono<Void>)
        Assert.notNull(publisher, "The given id must not be null!");
        return Mono.from(publisher).flatMap(
                this::deleteById);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Void>  delete(T entity) {
        Assert.notNull(entity, "The given id must not be null!");
        return operations.remove(entity).flatMap(res -> Mono.empty());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Void> deleteAll(Iterable<? extends T> entities) {
        Assert.notNull(entities, "The given Iterable of entities must not be null!");
        return operations
                .remove(entities)
                .last()
                .then(Mono.empty());
    }


    @Override
    public Mono<Void> deleteAll(Publisher<? extends T> entityStream) {
        Assert.notNull(entityStream, "The given publisher of entities must not be null!");
        return Flux.from(entityStream)
                .flatMap(entity -> delete(entity)).single();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Long> count() {
        N1QLExpression expression = select(x("COUNT(*)")).from(i(operations.getCouchbaseBucket().name()));
        QueryScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
        expression = addClassWhereClause(expression);
        N1QLQuery query = new N1QLQuery(expression, QueryOptions.queryOptions().scanConsistency(consistency));
        return operations.queryN1QL(query)
                .flatMapMany(res -> res.rowsAsObject()).single().map(row -> row.getLong("$1"));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Void> deleteAll() {
        N1QLExpression expression = x("DELETE").from(i(operations.getCouchbaseBucket().name()));
        QueryScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
        N1QLQuery query = new N1QLQuery(expression, QueryOptions.queryOptions().scanConsistency(consistency));
        return operations.queryN1QL(query).then(Mono.empty());
    }

    /**
     * Returns the information for the underlying template.
     *
     * @return the underlying entity information.
     */
    protected CouchbaseEntityInformation<T, String> getEntityInformation() {
        return entityInformation;
    }

    @Override
    public RxJavaCouchbaseOperations getCouchbaseOperations(){
        return operations;
    }

    private final N1QLExpression addClassWhereClause(N1QLExpression exp) {
        String classString = entityInformation.getJavaType().getCanonicalName();
        return exp.where(x("_class").eq(s(classString)));
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
