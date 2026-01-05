/*
 * Copyright 2012-present the original author or authors
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.couchbase.repository.DynamicProxyable;
import org.springframework.data.couchbase.repository.Query;
import org.springframework.data.couchbase.repository.ReactiveCouchbaseRepository;
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
public interface ReactiveUserColRepository
		extends ReactiveCouchbaseRepository<UserCol, String>, DynamicProxyable<ReactiveUserColRepository> {

	<S extends UserCol> Mono<S> save(S var1);

	Flux<UserCol> findByFirstname(String firstname);

	Flux<UserCol> findByFirstnameIn(String... firstnames);

	Flux<UserCol> findByFirstnameIn(JsonArray firstnames);

	Flux<UserCol> findByFirstnameAndLastname(String firstname, String lastname);

	@Query("#{#n1ql.selectEntity} where #{#n1ql.filter} and firstname = $1 and lastname = $2")
	Flux<UserCol> getByFirstnameAndLastname(String firstname, String lastname);

	@Query("#{#n1ql.selectEntity} where #{#n1ql.filter} and (firstname = $first or lastname = $last)")
	Flux<UserCol> getByFirstnameOrLastname(@Param("first") String firstname, @Param("last") String lastname);

	Flux<UserCol> findByIdIsNotNullAndFirstnameEquals(String firstname);

	Flux<UserCol> findByVersionEqualsAndFirstnameEquals(Long version, String firstname);

}
