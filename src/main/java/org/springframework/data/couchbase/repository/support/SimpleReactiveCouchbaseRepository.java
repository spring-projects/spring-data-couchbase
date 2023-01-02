/*
 * Copyright 2017-2023 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.repository.ReactiveCouchbaseRepository;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.domain.Sort;
import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

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

	@Override
	public Flux<T> findAll(Sort sort) {
		return findAll(new Query().with(sort));
	}

	@SuppressWarnings("unchecked")
	@Override
	public <S extends T> Mono<S> save(S entity) {
		return save(entity, getScope(), getCollection());
	}

	@Override
	public <S extends T> Flux<S> saveAll(Iterable<S> entities) {
		Assert.notNull(entities, "The given Iterable of entities must not be null!");
		String scope = getScope();
		String collection = getCollection();
		return Flux.fromIterable(entities).flatMap(e -> save(e, scope, collection));
	}

	@Override
	public <S extends T> Flux<S> saveAll(Publisher<S> entityStream) {
		Assert.notNull(entityStream, "The given Iterable of entities must not be null!");
		String scope = getScope();
		String collection = getCollection();
		return Flux.from(entityStream).flatMap(e -> save(e, scope, collection));
	}

	@SuppressWarnings("unchecked")
	private <S extends T> Mono<S> save(S entity, String scope, String collection) {
		Assert.notNull(entity, "Entity must not be null!");
		Mono<S> result;
		final CouchbasePersistentEntity<?> mapperEntity = operations.getConverter().getMappingContext()
				.getPersistentEntity(entity.getClass());
		final CouchbasePersistentProperty versionProperty = mapperEntity.getVersionProperty();
		final boolean versionPresent = versionProperty != null;
		final Long version = versionProperty == null || versionProperty.getField() == null ? null
				: (Long) ReflectionUtils.getField(versionProperty.getField(), entity);
		final boolean existingDocument = version != null && version > 0;

		if (!versionPresent) { // the entity doesn't have a version property
			// No version field - no cas
			result = (Mono<S>) operations.upsertById(getJavaType()).inScope(scope).inCollection(collection).one(entity);
		} else if (existingDocument) { // there is a version property, and it is non-zero
			// Updating existing document with cas
			result = (Mono<S>) operations.replaceById(getJavaType()).inScope(scope).inCollection(collection).one(entity);
		} else { // there is a version property, but it's zero or not set.
			// Creating new document
			result = (Mono<S>) operations.insertById(getJavaType()).inScope(scope).inCollection(collection).one(entity);
		}
		return result;
	}

	@Override
	public Mono<T> findById(ID id) {
		return findById(id, getScope(), getCollection());
	}

	@Override
	public Mono<T> findById(Publisher<ID> publisher) {
		Assert.notNull(publisher, "The given Publisher must not be null!");
		String scope = getScope();
		String collection = getCollection();
		return Mono.from(publisher).flatMap(id -> findById(id, scope, collection));
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
		String scope = getScope();
		String collection = getCollection();
		return Flux.from(entityStream).flatMap(id -> findById(id, scope, collection));
	}

	private Mono<T> findById(ID id, String scope, String collection) {
		return operations.findById(getJavaType()).inScope(scope).inCollection(collection).one(id.toString());
	}

	@Override
	public Mono<Boolean> existsById(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		return existsById(id, getScope(), getCollection());
	}

	private Mono<Boolean> existsById(ID id, String scope, String collection) {
		Assert.notNull(id, "The given id must not be null!");
		return operations.existsById(getJavaType()).inScope(scope).inCollection(collection).one(id.toString());
	}

	@Override
	public Mono<Boolean> existsById(Publisher<ID> publisher) {
		Assert.notNull(publisher, "The given Publisher must not be null!");
		String scope = getScope();
		String collection = getCollection();
		return Mono.from(publisher).flatMap(id -> existsById(id, scope, collection));
	}

	@Override
	public Flux<T> findAll() {
		return findAll(new Query());
	}

	@Override
	public Mono<Void> deleteById(ID id) {
		return deleteById(id, getScope(), getCollection());
	}

	private Mono<Void> deleteById(ID id, String scope, String collection) {
		return operations.removeById(getJavaType()).inScope(scope).inCollection(collection).one(id.toString()).then();
	}

	@Override
	public Mono<Void> deleteById(Publisher<ID> publisher) {
		Assert.notNull(publisher, "The given id must not be null!");
		String scope = getScope();
		String collection = getCollection();
		return Mono.from(publisher).flatMap(e -> deleteById(e, scope, collection));
	}

	@Override
	public Mono<Void> delete(T entity) {
		return delete(entity, getScope(), getCollection());
	}

	private Mono<Void> delete(T entity, String scope, String collection) {
		Assert.notNull(entity, "Entity must not be null!");
		return operations.removeById(getJavaType()).inScope(scope).inCollection(collection).one(getId(entity)).then();
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
		String scope = getScope();
		String collection = getCollection();
		return Flux.from(entityStream).flatMap(e -> delete(e, scope, collection)).single();
	}

	@Override
	public Mono<Void> deleteAll() {
		return operations.removeByQuery(getJavaType()).withConsistency(buildQueryScanConsistency()).inScope(getScope())
				.inCollection(getCollection()).all().then();
	}

	@Override
	public Mono<Long> count() {
		return operations.findByQuery(getJavaType()).withConsistency(buildQueryScanConsistency()).inScope(getScope())
				.inCollection(getCollection()).count();
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
