/*
 * Copyright 2012-2021 the original author or authors
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
import java.util.stream.Stream;

import com.couchbase.client.java.query.QueryScanConsistency;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.Query;
import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.couchbase.client.java.json.JsonArray;

/**
 * AbstractUser Repository for tests
 * 
 * @author Michael Reiche
 */
@Repository
@ScanConsistency(query=QueryScanConsistency.REQUEST_PLUS)
public interface AbstractUserRepository extends CouchbaseRepository<AbstractUser, String> {

	@Query("#{#n1ql.selectEntity} where (meta().id = $1)")
	AbstractUser myFindById(String id);

	List<User> findByFirstname(String firstname);

	Stream<User> findByLastname(String lastname);

	List<User> findByFirstnameIn(String... firstnames);

	List<User> findByFirstnameIn(JsonArray firstnames);

	List<User> findByFirstnameAndLastname(String firstname, String lastname);

	@Query("#{#n1ql.selectEntity} where #{#n1ql.filter} and firstname = $1 and lastname = $2")
	List<User> getByFirstnameAndLastname(String firstname, String lastname);

	@Query("#{#n1ql.selectEntity} where #{#n1ql.filter} and (firstname = $first or lastname = $last)")
	List<User> getByFirstnameOrLastname(@Param("first") String firstname, @Param("last") String lastname);

	List<User> findByIdIsNotNullAndFirstnameEquals(String firstname);

	List<User> findByVersionEqualsAndFirstnameEquals(Long version, String firstname);

}
