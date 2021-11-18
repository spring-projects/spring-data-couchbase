/*
 * Copyright 2017-2021 the original author or authors.
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

import static org.springframework.data.couchbase.repository.support.Util.hasNonZeroVersionProperty;

import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.repository.ReactiveCouchbaseRepository;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.domain.Sort;
import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;

/**
 * Reactive repository base implementation for Couchbase.
 *
 * @author Subhashni Balakrishnan
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author David Kelly
 * @author Douglas Six
 * @author Jens Schauder
 * @author Michael Reiche
 * @since 3.0
 */
public class SimpleReactiveCouchbaseRepository<T, ID> extends CouchbaseRepositoryBase<T, ID>
		implements ReactiveCouchbaseRepository<T, ID> {

	/**
	 * Holds the reference to the {@link CouchbaseOperations}.
	 */
	private final ReactiveCouchbaseOperations operations;

	/**
	 * Create a new Repository.
	 *
	 * @param entityInformation the Metadata for the entity.
	 * @param operations the reference to the reactive template used.
	 */
	public SimpleReactiveCouchbaseRepository(CouchbaseEntityInformation<T, String> entityInformation,
			ReactiveCouchbaseOperations operations, Class<?> repositoryInterface) {
		super(entityInformation, repositoryInterface);
		this.operations = operations;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <S extends T> Mono<S> save(S entity) {
		Assert.notNull(entity, "Entity must not be null!");
		// if entity has non-null, non-zero version property, then replace()
		Mono<S> result;
		// if there is a transaction, the entity must have a CAS, otherwise it will be inserted instead of replaced
		if (hasNonZeroVersionProperty(entity, operations.getConverter())) {
			result = (Mono<S>) operations.replaceById(getJavaType()).inScope(getScope()).inCollection(getCollection())
					.one(entity);
		} else if (((ReactiveCouchbaseTemplate) operations).getCtx() != null) { // tx does not have upsert, try insert
			result = (Mono<S>) operations.insertById(getJavaType()).inScope(getScope()).inCollection(getCollection())
					.one(entity);
		} else {
			result = (Mono<S>) operations.upsertById(getJavaType()).inScope(getScope()).inCollection(getCollection())
					.one(entity);
		}
		return result;
	}

	@Override
	public Flux<T> findAll(Sort sort) {
		return findAll(new Query().with(sort));
	}

	@Override
	public <S extends T> Flux<S> saveAll(Iterable<S> entities) {
		Assert.notNull(entities, "The given Iterable of entities must not be null!");
		return Flux.fromIterable(entities).flatMap(e -> save(e));
	}

	@Override
	public <S extends T> Flux<S> saveAll(Publisher<S> entityStream) {
		Assert.notNull(entityStream, "The given Iterable of entities must not be null!");
		return Flux.from(entityStream).flatMap(this::save);
	}

	@Override
	public Mono<T> findById(ID id) {
		return operations.findById(getJavaType()).inScope(getScope()).inCollection(getCollection()).one(id.toString());
	}

	@Override
	public Mono<T> findById(Publisher<ID> publisher) {
		Assert.notNull(publisher, "The given Publisher must not be null!");
		return Mono.from(publisher).flatMap(this::findById);
	}

	@Override
	public Mono<Boolean> existsById(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		return operations.existsById(getJavaType()).inScope(getScope()).inCollection(getCollection()).one(id.toString());
	}

	@Override
	public Mono<Boolean> existsById(Publisher<ID> publisher) {
		Assert.notNull(publisher, "The given Publisher must not be null!");
		return Mono.from(publisher).flatMap(this::existsById);
	}

	@Override
	public Flux<T> findAll() {
		return findAll(new Query());
	}

	@SuppressWarnings("unchecked")
	@Override
	public Flux<T> findAllById(Iterable<ID> ids) {
		Assert.notNull(ids, "The given Iterable of ids must not be null!");
		List<String> convertedIds = Streamable.of(ids).stream().map(Objects::toString).collect(Collectors.toList());
		return (Flux<T>) operations.findById(getJavaType()).inScope(getScope()).inCollection(getCollection())
				.all(convertedIds);
	}

	@Override
	public Flux<T> findAllById(Publisher<ID> entityStream) {
		Assert.notNull(entityStream, "The given entityStream must not be null!");
		return Flux.from(entityStream).flatMap(this::findById);
	}

	@Override
	public Mono<Void> deleteById(ID id) {
		return operations.removeById(getJavaType()).inScope(getScope()).inCollection(getCollection()).one(id.toString())
				.then();
	}

	@Override
	public Mono<Void> deleteById(Publisher<ID> publisher) {
		Assert.notNull(publisher, "The given id must not be null!");
		return Mono.from(publisher).flatMap(this::deleteById);
	}

	@Override
	public Mono<Void> delete(T entity) {
		Assert.notNull(entity, "Entity must not be null!");
		return operations.removeById(getJavaType()).inScope(getScope()).inCollection(getCollection()).one(getId(entity))
				.then();
	}

	@Override
	public Mono<Void> deleteAllById(Iterable<? extends ID> ids) {
		return operations.removeById(getJavaType()).inScope(getScope()).inCollection(getCollection())
				.all(Streamable.of(ids).map(Object::toString).toList()).then();
	}

	@Override
	public Mono<Void> deleteAll(Iterable<? extends T> entities) {
		return operations.removeById(getJavaType()).inScope(getScope()).inCollection(getCollection())
				.all(Streamable.of(entities).map(this::getId).toList()).then();
	}

	@Override
	public Mono<Void> deleteAll(Publisher<? extends T> entityStream) {
		Assert.notNull(entityStream, "The given publisher of entities must not be null!");
		return Flux.from(entityStream).flatMap(this::delete).single();
	}

	@Override
	public Mono<Long> count() {
		return operations.findByQuery(getJavaType()).withConsistency(buildQueryScanConsistency()).inScope(getScope())
				.inCollection(getCollection()).count();
	}

	@Override
	public Mono<Void> deleteAll() {
		return operations.removeByQuery(getJavaType()).withConsistency(buildQueryScanConsistency()).inScope(getScope())
				.inCollection(getCollection()).all().then();
	}

	private Flux<T> findAll(Query query) {
		return operations.findByQuery(getJavaType()).withConsistency(buildQueryScanConsistency()).inScope(getScope())
				.inCollection(getCollection()).matching(query).all();
	}

	@Override
	public ReactiveCouchbaseOperations getOperations() {
		return operations;
	}

}
