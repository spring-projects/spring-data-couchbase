/*
 * Copyright 2017-2020 the original author or authors.
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

import static org.springframework.data.couchbase.repository.support.Util.*;

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

import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * Reactive repository base implementation for Couchbase.
 *
 * @author Subhashni Balakrishnan
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author David Kelly
 * @author Douglas Six
 * @author Jens Schauder
 * @since 3.0
 */
public class SimpleReactiveCouchbaseRepository<T, ID> implements ReactiveCouchbaseRepository<T, ID> {

	/**
	 * Holds the reference to the {@link CouchbaseOperations}.
	 */
	private final ReactiveCouchbaseOperations operations;

	/**
	 * Contains information about the entity being used in this repository.
	 */
	private final CouchbaseEntityInformation<T, String> entityInformation;

	private CrudMethodMetadata crudMethodMetadata;

	/**
	 * Create a new Repository.
	 *
	 * @param entityInformation the Metadata for the entity.
	 * @param operations the reference to the reactive template used.
	 */
	public SimpleReactiveCouchbaseRepository(CouchbaseEntityInformation<T, String> entityInformation,
			ReactiveCouchbaseOperations operations) {
		Assert.notNull(operations, "ReactiveCouchbaseOperations must not be null!");
		Assert.notNull(entityInformation, "CouchbaseEntityInformation must not be null!");

		this.entityInformation = entityInformation;
		this.operations = operations;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <S extends T> Mono<S> save(S entity) {
		Assert.notNull(entity, "Entity must not be null!");
		// if entity has non-null version property, then replace()
		if (hasNonZeroVersionProperty(entity, operations.getConverter())) {
			return (Mono<S>) operations.replaceById(entityInformation.getJavaType()).one(entity);
		} else {
			return (Mono<S>) operations.upsertById(entityInformation.getJavaType()).one(entity);
		}
	}

	@Override
	public Flux<T> findAll(Sort sort) {
		return findAll(new Query().with(sort));
	}

	@Override
	public <S extends T> Flux<S> saveAll(Iterable<S> entities) {
		Assert.notNull(entities, "The given Iterable of entities must not be null!");
		return Flux.fromIterable(entities).flatMap(this::save);
	}

	@Override
	public <S extends T> Flux<S> saveAll(Publisher<S> entityStream) {
		Assert.notNull(entityStream, "The given Iterable of entities must not be null!");
		return Flux.from(entityStream).flatMap(this::save);
	}

	@Override
	public Mono<T> findById(ID id) {
		return operations.findById(entityInformation.getJavaType()).one(id.toString());
	}

	@Override
	public Mono<T> findById(Publisher<ID> publisher) {
		Assert.notNull(publisher, "The given Publisher must not be null!");
		return Mono.from(publisher).flatMap(this::findById);
	}

	@Override
	public Mono<Boolean> existsById(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		return operations.existsById().one(id.toString());
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
		return (Flux<T>) operations.findById(entityInformation.getJavaType()).all(convertedIds);
	}

	@Override
	public Flux<T> findAllById(Publisher<ID> entityStream) {
		Assert.notNull(entityStream, "The given entityStream must not be null!");
		return Flux.from(entityStream).flatMap(this::findById);
	}

	@Override
	public Mono<Void> deleteById(ID id) {
		return operations.removeById().one(id.toString()).then();
	}

	@Override
	public Mono<Void> deleteById(Publisher<ID> publisher) {
		Assert.notNull(publisher, "The given id must not be null!");
		return Mono.from(publisher).flatMap(this::deleteById);
	}

	@Override
	public Mono<Void> delete(T entity) {
		Assert.notNull(entity, "Entity must not be null!");
		return operations.removeById().one(entityInformation.getId(entity)).then();
	}

	@Override
	public Mono<Void> deleteAllById(Iterable<? extends ID> ids) {
		return operations.removeById().all(Streamable.of(ids).map(Object::toString).toList()).then();
	}

	@Override
	public Mono<Void> deleteAll(Iterable<? extends T> entities) {
		return operations.removeById().all(Streamable.of(entities).map(entityInformation::getId).toList()).then();
	}

	@Override
	public Mono<Void> deleteAll(Publisher<? extends T> entityStream) {
		Assert.notNull(entityStream, "The given publisher of entities must not be null!");
		return Flux.from(entityStream).flatMap(this::delete).single();
	}

	@Override
	public Mono<Long> count() {
		return operations.findByQuery(entityInformation.getJavaType()).withConsistency(buildQueryScanConsistency()).count();
	}

	@Override
	public Mono<Void> deleteAll() {
		return operations.removeByQuery(entityInformation.getJavaType()).withConsistency(buildQueryScanConsistency()).all().then();
	}

	/**
	 * Returns the information for the underlying template.
	 *
	 * @return the underlying entity information.
	 */
	protected CouchbaseEntityInformation<T, String> getEntityInformation() {
		return entityInformation;
	}

	private Flux<T> findAll(Query query) {
		return operations.findByQuery(entityInformation.getJavaType()).withConsistency(buildQueryScanConsistency())
				.matching(query).all();
	}

	private QueryScanConsistency buildQueryScanConsistency() {
		QueryScanConsistency scanConsistency = QueryScanConsistency.NOT_BOUNDED;
		if (crudMethodMetadata.getScanConsistency() != null) {
			scanConsistency = crudMethodMetadata.getScanConsistency().query();
		}
		return scanConsistency;
	}

	/**
	 * Setter for the repository metadata, contains annotations on the overidden methods.
	 *
	 * @param crudMethodMetadata the injected repository metadata.
	 */
	void setRepositoryMethodMetadata(CrudMethodMetadata crudMethodMetadata) {
		this.crudMethodMetadata = crudMethodMetadata;
	}

}
