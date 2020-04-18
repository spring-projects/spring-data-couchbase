/*
 * Copyright 2013-2020 the original author or authors.
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

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.util.StreamUtils;
import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;

import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * Repository base implementation for Couchbase.
 *
 * @author Michael Nitschinger
 * @author Mark Paluch
 */
public class SimpleCouchbaseRepository<T, ID> implements CouchbaseRepository<T, ID> {

	/**
	 * Holds the reference to the {@link org.springframework.data.couchbase.core.CouchbaseTemplate}.
	 */
	private final CouchbaseOperations couchbaseOperations;

	/**
	 * Contains information about the entity being used in this repository.
	 */
	private final CouchbaseEntityInformation<T, String> entityInformation;

	private CrudMethodMetadata crudMethodMetadata;

	/**
	 * Create a new Repository.
	 *
	 * @param entityInformation the Metadata for the entity.
	 * @param couchbaseOperations the reference to the template used.
	 */
	public SimpleCouchbaseRepository(final CouchbaseEntityInformation<T, String> entityInformation,
			final CouchbaseOperations couchbaseOperations) {
		Assert.notNull(entityInformation, "CouchbaseEntityInformation must not be null!");
		Assert.notNull(couchbaseOperations, "CouchbaseOperations must not be null!");

		this.entityInformation = entityInformation;
		this.couchbaseOperations = couchbaseOperations;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <S extends T> S save(final S entity) {
		Assert.notNull(entity, "Entity must not be null!");
		return (S) couchbaseOperations.upsertById(entityInformation.getJavaType()).one(entity);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <S extends T> Iterable<S> saveAll(final Iterable<S> entities) {
		Assert.notNull(entities, "The given Iterable of entities must not be null!");
		return (Iterable<S>) couchbaseOperations.upsertById(entityInformation.getJavaType())
				.all(Streamable.of(entities).toList());
	}

	@Override
	public Optional<T> findById(final ID id) {
		Assert.notNull(id, "The given id must not be null!");
		return Optional.ofNullable(couchbaseOperations.findById(entityInformation.getJavaType()).one(id.toString()));
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<T> findAllById(final Iterable<ID> ids) {
		Assert.notNull(ids, "The given Iterable of ids must not be null!");
		List<String> convertedIds = Streamable.of(ids).stream().map(Objects::toString).collect(Collectors.toList());
		Collection<? extends T> all = couchbaseOperations.findById(entityInformation.getJavaType()).all(convertedIds);
		return Streamable.of(all).stream().collect(StreamUtils.toUnmodifiableList());
	}

	@Override
	public boolean existsById(final ID id) {
		Assert.notNull(id, "The given id must not be null!");
		return couchbaseOperations.existsById().one(id.toString());
	}

	@Override
	public void deleteById(final ID id) {
		Assert.notNull(id, "The given id must not be null!");
		couchbaseOperations.removeById().one(id.toString());
	}

	@Override
	public void delete(final T entity) {
		Assert.notNull(entity, "Entity must not be null!");
		couchbaseOperations.removeById().one(entityInformation.getId(entity));
	}

	@Override
	public void deleteAll(final Iterable<? extends T> entities) {
		Assert.notNull(entities, "The given Iterable of entities must not be null!");
		couchbaseOperations.removeById().all(Streamable.of(entities).map(entityInformation::getId).toList());
	}

	@Override
	public long count() {
		return couchbaseOperations.findByQuery(entityInformation.getJavaType()).consistentWith(buildQueryScanConsistency())
				.count();
	}

	@Override
	public void deleteAll() {
		couchbaseOperations.removeByQuery(entityInformation.getJavaType()).consistentWith(buildQueryScanConsistency())
				.all();
	}

	@Override
	public List<T> findAll() {
		return findAll(new Query());
	}

	@Override
	public List<T> findAll(final Sort sort) {
		return findAll(new Query().with(sort));
	}

	@Override
	public Page<T> findAll(final Pageable pageable) {
		List<T> results = findAll(new Query().with(pageable));
		return new PageImpl<>(results, pageable, count());
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
	 * Helper method to assemble a n1ql find all query, taking annotations into acocunt.
	 *
	 * @param query the originating query.
	 * @return the list of found entities, already executed.
	 */
	private List<T> findAll(final Query query) {
		return couchbaseOperations.findByQuery(entityInformation.getJavaType()).consistentWith(buildQueryScanConsistency())
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
	void setRepositoryMethodMetadata(final CrudMethodMetadata crudMethodMetadata) {
		this.crudMethodMetadata = crudMethodMetadata;
	}

}
