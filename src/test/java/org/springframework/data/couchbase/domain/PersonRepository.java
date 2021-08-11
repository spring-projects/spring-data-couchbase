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
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.couchbase.repository.Query;
import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * @author Michael Reiche
 */
public interface PersonRepository extends CrudRepository<Person, String> {

	/*
	 * These methods are exercised in HomeController of the test spring-boot DemoApplication
	 */

	public List<Person> findByLastname(String lastname);

	@Query("#{#n1ql.selectEntity} where firstname = 'Reba' and lastname = $last")
	public List<Person> any(@Param("last") String any);

	@Query("#{#n1ql.selectEntity} where firstname = 'Reba' and lastname = 'McIntyre'")
	public List<Person> none();

	@Query("#{#n1ql.selectEntity} where firstname = 'Reba' and lastname = $1")
	public List<Person> one(@Param("last") String any);

	@Query("#{#n1ql.selectEntity} where lastname = $2 and firstname = $1")
	public List<Person> two(@Param("one") String one, @Param("two") String two);

	@Query("#{#n1ql.selectEntity} where lastname in ( $1 )")
	public List<Person> lastnameIn(@Param("lastnames") String[] lastnames);

	public Person findByFirstname(String firstname);

	public Person findFromReplicasByFirstname(String firstname);

	public List<Person> findFromReplicasById(String id);

	public List<Person> findByFirstnameLike(String firstname);

	public List<Person> findByFirstnameIsNull();

	public List<Person> findByFirstnameIsNotNull();

	public List<Person> findByFirstnameNotLike(String firstname);

	public List<Person> findByFirstnameStartingWith(String firstname);

	public List<Person> findByFirstnameEndingWith(String firstname);

	public List<Person> findByFirstnameContaining(String firstname);

	public List<Person> findByFirstnameNotContaining(String firstname);

	public List<Person> findByFirstnameBetween(String firstname1, String firstname2);

	public List<Person> findByFirstnameIn(String... firstnames);

	public List<Person> findByFirstnameNotIn(String... firstnames);

	public List<Person> findByFirstnameTrue(Object... o);

	public List<Person> findByFirstnameFalse(Object... o);

	List<Person> findByFirstnameAndLastname(String firstname, String lastname);

	List<Person> findByFirstnameOrLastname(String firstname, String lastname);

	<S extends Person> S save(S var1);

	<S extends Person> Iterable<S> saveAll(Iterable<S> var1);

	Optional<Person> findById(UUID var1);

	boolean existsById(UUID var1);

	Iterable<Person> findAll();

	long count();

	void deleteById(UUID var1);

	void delete(Person var1);

	void deleteAll(Iterable<? extends Person> var1);

	void deleteAll();

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	List<Person> findByAddressStreet(String street);

	@ScanConsistency(query = QueryScanConsistency.REQUEST_PLUS)
	List<Person> findByMiddlename(String nickName);
}
