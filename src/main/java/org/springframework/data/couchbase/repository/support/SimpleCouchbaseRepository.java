/*
 * Copyright 2013-2021 the original author or authors.
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

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
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
 * @author Jens Schauder
 * @author Michael Reiche
 */
public class SimpleCouchbaseRepository<T, ID> extends CouchbaseRepositoryBase<T, ID>
		implements CouchbaseRepository<T, ID> {

	/**
	 * Holds the reference to the {@link org.springframework.data.couchbase.core.CouchbaseTemplate}.
	 */
	private final CouchbaseOperations operations;

	/**
	 * Create a new Repository.
	 *
	 * @param entityInformation the Metadata for the entity.
	 * @param couchbaseOperations the reference to the template used.
	 * @param repositoryInterface the repository interface being fronted
	 */
	public SimpleCouchbaseRepository(CouchbaseEntityInformation<T, String> entityInformation,
			CouchbaseOperations couchbaseOperations, Class<?> repositoryInterface) {
		super(entityInformation, repositoryInterface);
		this.operations = couchbaseOperations;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <S extends T> S save(S entity) {
		Assert.notNull(entity, "Entity must not be null!");
		// if entity has non-null, non-zero version property, then replace()
		S result;
		// if there is a transaction, the entity must have a CAS, otherwise it will be inserted instead of replaced
		if (hasNonZeroVersionProperty(entity, operations.getConverter())) {
			result = (S) operations.replaceById(getJavaType()).inScope(getScope()).inCollection(getCollection()).one(entity);
		} else if (((CouchbaseTemplate) operations).reactive().getCtx() != null) { // tx does not have upsert, try insert
			result = (S) operations.insertById(getJavaType()).inScope(getScope()).inCollection(getCollection()).one(entity);
		} else {
			result = (S) operations.upsertById(getJavaType()).inScope(getScope()).inCollection(getCollection()).one(entity);
		}
		return result;
	}

	@Override
	public <S extends T> Iterable<S> saveAll(Iterable<S> entities) {
		Assert.notNull(entities, "The given Iterable of entities must not be null!");
		return Streamable.of(entities).stream().map((e) -> save(e)).collect(StreamUtils.toUnmodifiableList());
	}

	@Override
	public Optional<T> findById(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		return Optional.ofNullable(
				operations.findById(getJavaType()).inScope(getScope()).inCollection(getCollection()).one(id.toString()));
	}

	@Override
	public List<T> findAllById(Iterable<ID> ids) {
		Assert.notNull(ids, "The given Iterable of ids must not be null!");
		List<String> convertedIds = Streamable.of(ids).stream().map(Objects::toString).collect(Collectors.toList());
		Collection<? extends T> all = operations.findById(getJavaType()).inScope(getScope()).inCollection(getCollection())
				.all(convertedIds);
		return Streamable.of(all).stream().collect(StreamUtils.toUnmodifiableList());
	}

	@Override
	public boolean existsById(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		return operations.existsById(getJavaType()).inScope(getScope()).inCollection(getCollection()).one(id.toString());
	}

	@Override
	public void deleteById(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		operations.removeById(getJavaType()).inScope(getScope()).inCollection(getCollection()).one(id.toString());
	}

	@Override
	public void delete(T entity) {
		Assert.notNull(entity, "Entity must not be null!");
		if (((CouchbaseTemplate) operations).reactive().getCtx() == null) {
			operations.removeById(getJavaType()).inScope(getScope()).inCollection(getCollection()).one(getId(entity));
		} else {
			operations.removeById(getJavaType()).inScope(getScope()).inCollection(getCollection()).one(entity);
		}
	}

	@Override
	public void deleteAllById(Iterable<? extends ID> ids) {
		Assert.notNull(ids, "The given Iterable of ids must not be null!");
		operations.removeById(getJavaType()).inScope(getScope()).inCollection(getCollection())
				.all(Streamable.of(ids).map(Objects::toString).toList());
	}

	@Override
	public void deleteAll(Iterable<? extends T> entities) {
		Assert.notNull(entities, "The given Iterable of entities must not be null!");
		operations.removeById(getJavaType()).inScope(getScope()).inCollection(getCollection())
				.all(Streamable.of(entities).map(this::getId).toList());
	}

	@Override
	public long count() {
		return operations.findByQuery(getJavaType()).withConsistency(buildQueryScanConsistency()).inScope(getScope())
				.inCollection(getCollection()).count();
	}

	@Override
	public void deleteAll() {
		operations.removeByQuery(getJavaType()).withConsistency(buildQueryScanConsistency()).inScope(getScope())
				.inCollection(getCollection()).all();
	}

	@Override
	public List<T> findAll() {
		return findAll(new Query());
	}

	@Override
	public List<T> findAll(Sort sort) {
		return findAll(new Query().with(sort));
	}

	@Override
	public List<T> findAll(QueryScanConsistency queryScanConsistency) {
		return findAll(new Query().scanConsistency(queryScanConsistency));
	}

	@Override
	public Page<T> findAll(Pageable pageable) {
		List<T> results = findAll(new Query().with(pageable));
		return new PageImpl<>(results, pageable, count());
	}

	/**
	 * Helper method to assemble a n1ql find all query, taking annotations into acocunt.
	 *
	 * @param query the originating query.
	 * @return the list of found entities, already executed.
	 */
	private List<T> findAll(Query query) {
		return operations.findByQuery(getJavaType()).withConsistency(buildQueryScanConsistency()).inScope(getScope())
				.inCollection(getCollection()).matching(query).all();
	}

	@Override
	public CouchbaseOperations getOperations() {
		return operations;
	}

}
