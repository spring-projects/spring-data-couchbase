/*
 * Copyright 2013-2025 the original author or authors.
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

package org.springframework.data.couchbase.repository;

import java.util.List;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.Repository;

import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * Couchbase specific {@link Repository} interface.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@NoRepositoryBean
public interface CouchbaseRepository<T, ID> extends PagingAndSortingRepository<T, ID>, CrudRepository<T, ID> {

	@Override
	List<T> findAll(Sort sort);

	List<T> findAll(QueryScanConsistency queryScanConsistency);

	@Override
	List<T> findAll();

	@Override
	List<T> findAllById(Iterable<ID> iterable);

	CouchbaseEntityInformation<T, String> getEntityInformation();

	CouchbaseOperations getOperations();

}
