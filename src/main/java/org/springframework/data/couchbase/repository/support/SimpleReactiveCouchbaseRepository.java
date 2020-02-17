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

import org.reactivestreams.Publisher;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.repository.ReactiveCouchbaseRepository;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.domain.Sort;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
public class SimpleReactiveCouchbaseRepository<T, ID> implements ReactiveCouchbaseRepository<T, ID> {

    /**
     * Holds the reference to the {@link CouchbaseOperations}.
     */
    private final CouchbaseOperations operations;

    /**
     * Contains information about the entity being used in this repository.
     */
    private final CouchbaseEntityInformation<T, String> entityInformation;

    /**
     * Create a new Repository.
     *
     * @param metadata the Metadata for the entity.
     * @param operations the reference to the reactive template used.
     */
    public SimpleReactiveCouchbaseRepository(final CouchbaseEntityInformation<T, String> metadata,
                                             final CouchbaseOperations operations) {
        Assert.notNull(operations, "RxJavaCouchbaseOperations must not be null!");
        Assert.notNull(metadata, "CouchbaseEntityInformation must not be null!");

        this.entityInformation = metadata;
        this.operations = operations;
    }

    @SuppressWarnings("unchecked")
    public <S extends T> Mono<S> save(S entity) {
        Assert.notNull(entity, "Entity must not be null!");
        throw new UnsupportedOperationException("TODO");
        //return operations.save(entity);
    }

    @Override
    public Flux<T> findAll(Sort sort) {
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S extends T> Flux<S> saveAll(Iterable<S> entities) {
        Assert.notNull(entities, "The given Iterable of entities must not be null!");
        throw new UnsupportedOperationException("TODO");

//        return operations.save(entities);
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
        throw new UnsupportedOperationException("TODO");

       // Assert.notNull(id, "The given id must not be null!");
/*        return operations.findById(id.toString(), entityInformation.getJavaType())
                .onErrorResume(throwable -> {
                    //reactive streams adapter doesn't work with null
                    if(throwable instanceof NullPointerException) {
                        return Mono.empty();
                    }
                    return Mono.error(throwable);
                });*/
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
    public Mono<Boolean> existsById(final ID id) {
        Assert.notNull(id, "The given id must not be null!");
        return operations.existsById().reactive().one(id.toString());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Boolean> existsById(final Publisher<ID> publisher) {
        Assert.notNull(publisher, "The given Publisher must not be null!");
        return Mono.from(publisher).flatMap(this::existsById);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Flux<T> findAll() {
        throw new UnsupportedOperationException("TODO");

        // TODO: figure out a cleaner way
/*        N1QLExpression expression = N1qlUtils.createSelectFromForEntity(operations.getCouchbaseBucket().name());
        QueryScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
        expression = addClassWhereClause(expression);
        N1QLQuery query = new N1QLQuery(expression, QueryOptions.queryOptions().scanConsistency(consistency));

        return operations.findByN1QL(query, entityInformation.getJavaType());*/
    }

    @SuppressWarnings("unchecked")
    @Override
    public Flux<T> findAllById(final Iterable<ID> ids) {
        throw new UnsupportedOperationException("TODO");

/*        // TODO: figure out a cleaner way
        N1QLExpression expression = N1qlUtils.createSelectFromForEntity(operations.getCouchbaseBucket().name());
        expression = expression.keys(ids);
        expression = addClassWhereClause(expression);
        QueryScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
        N1QLQuery query = new N1QLQuery(expression, QueryOptions.queryOptions().scanConsistency(consistency));
        return operations.findByN1QL(query, entityInformation.getJavaType());*/
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
        throw new UnsupportedOperationException("TODO");

        // TODO: why does this not match the operations (Mono<T> vs Mono<Void>)
/*        Assert.notNull(id, "The given id must not be null!");
        return operations.remove(id.toString()).flatMap(res-> Mono.empty());*/
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
        throw new UnsupportedOperationException("TODO");

/*        Assert.notNull(entity, "The given id must not be null!");
        return operations.remove(entity).flatMap(res -> Mono.empty());*/
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Void> deleteAll(Iterable<? extends T> entities) {
        throw new UnsupportedOperationException("TODO");

/*        Assert.notNull(entities, "The given Iterable of entities must not be null!");
        return operations
                .remove(entities)
                .last()
                .then(Mono.empty());*/
    }


    @Override
    public Mono<Void> deleteAll(Publisher<? extends T> entityStream) {
        Assert.notNull(entityStream, "The given publisher of entities must not be null!");
        return Flux.from(entityStream).flatMap(this::delete).single();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Long> count() {
        throw new UnsupportedOperationException("TODO");

/*        N1QLExpression expression = select(x("COUNT(*)")).from(i(operations.getCouchbaseBucket().name()));
        QueryScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
        expression = addClassWhereClause(expression);
        N1QLQuery query = new N1QLQuery(expression, QueryOptions.queryOptions().scanConsistency(consistency));
        return operations.queryN1QL(query)
                .flatMapMany(res -> res.rowsAsObject()).single().map(row -> row.getLong("$1"));*/
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Void> deleteAll() {
        throw new UnsupportedOperationException("TODO");

/*        N1QLExpression expression = x("DELETE").from(i(operations.getCouchbaseBucket().name()));
        QueryScanConsistency consistency = getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();
        N1QLQuery query = new N1QLQuery(expression, QueryOptions.queryOptions().scanConsistency(consistency));
        return operations.queryN1QL(query).then(Mono.empty());*/
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
    public CouchbaseOperations getCouchbaseOperations(){
        return operations;
    }

}
