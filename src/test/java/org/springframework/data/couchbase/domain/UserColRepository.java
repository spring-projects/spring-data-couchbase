/*
 * Copyright 2012-2025 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.domain;

import java.util.List;

import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.DynamicProxyable;
import org.springframework.data.couchbase.repository.Query;
import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * User Repository for tests
 * 
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@Repository
@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
public interface UserColRepository extends CouchbaseRepository<UserCol, String>, DynamicProxyable<UserColRepository> {

	// CouchbaseRepositoryQueryCollectionIntegrationTests.testScopeCollectionAnnotationSwap() relies on this
	// being commented out.
	// <S extends UserCol> S save(S var1);

	List<UserCol> findByFirstname(String firstname);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	UserCol getById(String id);

	List<UserCol> findByFirstnameIn(String... firstnames);

	List<UserCol> findByFirstnameIn(JsonArray firstnames);

	List<UserCol> findByFirstnameAndLastname(String firstname, String lastname);

	@Query("#{#n1ql.selectEntity} where #{#n1ql.filter} and firstname = $1 and lastname = $2")
	List<UserCol> getByFirstnameAndLastname(String firstname, String lastname);

	@Query("#{#n1ql.selectEntity} where #{#n1ql.filter} and (firstname = $first or lastname = $last)")
	List<UserCol> getByFirstnameOrLastname(@Param("first") String firstname, @Param("last") String lastname);

	List<UserCol> findByIdIsNotNullAndFirstnameEquals(String firstname);

	List<UserCol> findByVersionEqualsAndFirstnameEquals(Long version, String firstname);

}
