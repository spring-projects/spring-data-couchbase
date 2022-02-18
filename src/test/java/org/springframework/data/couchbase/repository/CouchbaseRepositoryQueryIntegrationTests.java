/*
 * Copyright 2017-2022 the original author or authors.
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

import static com.couchbase.client.java.query.QueryScanConsistency.NOT_BOUNDED;
import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.data.couchbase.config.BeanNames.COUCHBASE_TEMPLATE;

import junit.framework.AssertionFailedError;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.validation.ConstraintViolationException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseQueryExecutionException;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.core.mapping.event.ValidatingCouchbaseEventListener;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.domain.Address;
import org.springframework.data.couchbase.domain.AirlineRepository;
import org.springframework.data.couchbase.domain.Airport;
import org.springframework.data.couchbase.domain.AirportMini;
import org.springframework.data.couchbase.domain.AirportRepository;
import org.springframework.data.couchbase.domain.AirportRepositoryScanConsistencyTest;
import org.springframework.data.couchbase.domain.Iata;
import org.springframework.data.couchbase.domain.NaiveAuditorAware;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonRepository;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserAnnotated;
import org.springframework.data.couchbase.domain.UserRepository;
import org.springframework.data.couchbase.domain.UserSubmission;
import org.springframework.data.couchbase.domain.UserSubmissionRepository;
import org.springframework.data.couchbase.domain.time.AuditingDateTimeProvider;
import org.springframework.data.couchbase.repository.auditing.EnableCouchbaseAuditing;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.query.CouchbaseQueryMethod;
import org.springframework.data.couchbase.repository.query.CouchbaseRepositoryQuery;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import com.couchbase.client.core.error.AmbiguousTimeoutException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.IndexFailureException;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * Repository tests
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Jens Schauder
 */
@SpringJUnitConfig(CouchbaseRepositoryQueryIntegrationTests.Config.class)
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
public class CouchbaseRepositoryQueryIntegrationTests extends ClusterAwareIntegrationTests {

	@Autowired CouchbaseClientFactory clientFactory;

	@Autowired AirportRepository airportRepository;

	@Autowired AirlineRepository airlineRepository;

	@Autowired UserRepository userRepository;

	@Autowired UserSubmissionRepository userSubmissionRepository;

	@Autowired CouchbaseTemplate couchbaseTemplate;

	String scopeName = "_default";
	String collectionName = "_default";

	@BeforeEach
	public void beforeEach() {
		super.beforeEach();
		couchbaseTemplate.removeByQuery(User.class).withConsistency(REQUEST_PLUS).all();
		couchbaseTemplate.findByQuery(User.class).withConsistency(REQUEST_PLUS).all();
	}

	@Test
	void shouldSaveAndFindAll() {
		Airport vie = null;
		try {
			vie = new Airport("airports::vie", "vie", "low4");
			vie.setSize(2);
			airportRepository.save(vie);
			List<Airport> all = new ArrayList<>();
			airportRepository.findAll().forEach(all::add);
			assertFalse(all.isEmpty());
			assertTrue(all.stream().anyMatch(a -> a.getId().equals("airports::vie")));
		} finally {
			airportRepository.delete(vie);
		}
	}

	@Test
	void shouldNotSave() {
		Airport vie = new Airport("airports::vie", "vie", "low4");
		vie.setSize(3);
		try {
			assertThrows(ConstraintViolationException.class, () -> airportRepository.save(vie));
		} catch (AssertionFailedError e) {
			airportRepository.delete(vie);
			throw e;
		}
	}

	@Autowired PersonRepository personRepository;

	@Test
	void nestedFind() {
		Person person = null;
		try {
			person = new Person(1, "first", "last");
			Address address = new Address();
			address.setStreet("Maple");
			person.setAddress(address);
			personRepository.save(person);
			List<Person> persons = personRepository.findByAddressStreet("Maple");
			assertEquals(1, persons.size());
			List<Person> persons2 = personRepository.findByMiddlename("Nick");
			assertEquals(1, persons2.size());
		} finally {
			personRepository.deleteById(person.getId().toString());
		}
	}

	@Test
	void annotatedFieldFind() {
		Person person = null;
		try {
			person = new Person(1, "first", "last");
			person.setMiddlename("Nick"); // middlename is stored as nickname
			personRepository.save(person);
			Person person2 = personRepository.findById(person.getId().toString()).get();
			assertEquals(person.getMiddlename(), person2.getMiddlename());
			List<Person> persons3 = personRepository.findByMiddlename("Nick");
			assertEquals(1, persons3.size());
			assertEquals(person.getMiddlename(), persons3.get(0).getMiddlename());
		} finally {
			personRepository.deleteById(person.getId().toString());
		}
	}

	@Test
	void annotatedFieldFindName() {
		Person person = null;
		try {
			person = new Person(1, "first", "last");
			person.setSalutation("Mrs"); // salutation is stored as prefix
			personRepository.save(person);
			GetResult result = couchbaseTemplate.getCouchbaseClientFactory().getBucket().defaultCollection()
					.get(person.getId().toString());
			assertEquals(person.getSalutation(), result.contentAsObject().get("prefix"));
			Person person2 = personRepository.findById(person.getId().toString()).get();
			assertEquals(person.getSalutation(), person2.getSalutation());
			List<Person> persons3 = personRepository.findBySalutation("Mrs");
			assertEquals(1, persons3.size());
			assertEquals(person.getSalutation(), persons3.get(0).getSalutation());
		} finally {
			personRepository.deleteById(person.getId().toString());
		}
	}

	// "1\" or name=name or name=\"1")
	@Test
	void findByInjection() {
		Airport vie = null;
		Airport xxx = null;
		try {
			vie = new Airport("airports::vie", "vie", "low5");
			airportRepository.save(vie);
			xxx = new Airport("airports::xxx", "xxx", "xxxx");
			airportRepository.save(xxx);
			List<Airport> airports;
			airports = airportRepository.findAllByIata("1\" or iata=iata or iata=\"1");
			assertEquals(0, airports.size());
			airports = airportRepository.findAllByIata("vie");
			assertEquals(1, airports.size());
		} finally {
			airportRepository.delete(vie);
			airportRepository.delete(xxx);
		}

	}

	@Test
	void issue1304CollectionParameter() {
		Airport vie = null;
		try {
			vie = new Airport("airports::vie", "vie", "low5");
			airportRepository.save(vie);
			java.util.Collection<String> iatas = new LinkedList<String>();
			iatas.add(vie.getIata());
			java.util.Collection<String> icaos = new LinkedList<String>();
			icaos.add(vie.getIcao());
			icaos.add("blue");
			PageRequest pageable = PageRequest.of(0, 1, Sort.by("iata"));
			List<Airport> airports = airportRepository.findByIataInAndIcaoIn(iatas, icaos, pageable);
			assertEquals(1, airports.size());

			List<Airport> airports2 = airportRepository.findByIataInAndIcaoIn(iatas, icaos, pageable);
			assertEquals(1, airports2.size());

		} finally {
			airportRepository.delete(vie);
		}

	}

	@Test
	void findBySimpleProperty() {
		Airport vie = null;
		try {
			vie = new Airport("airports::vie", "vie", "low6");
			vie = airportRepository.save(vie);
			Airport airport2 = airportRepository
					.withOptions(QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS))
					.findByIata(vie.getIata());
			assertEquals(airport2, vie);

			List<Airport> airports = airportRepository.findAllByIata("vie");
			assertEquals(1, airports.size());
			Airport airport1 = airportRepository.findById(airports.get(0).getId()).get();
			assertEquals(airport1.getIata(), vie.getIata());
			airport2 = airportRepository.findByIata(airports.get(0).getIata());
			assertEquals(airport2.getId(), vie.getId());
		} finally {
			airportRepository.delete(vie);
		}
	}

	@Test
	void findBySimplePropertyReturnType() {
		Airport vie = null;
		try {
			vie = new Airport("airports::vie", "vie", "low6");
			vie = airportRepository.save(vie);
			List<AirportMini> airports = airportRepository.getByIata("vie");
			assertEquals(1, airports.size());
			System.out.println(airports.get(0));
		} finally {
			airportRepository.delete(vie);
		}
	}

	@Test
	public void saveNotBoundedRequestPlus() {
		airportRepository.withOptions(QueryOptions.queryOptions().scanConsistency(REQUEST_PLUS)).deleteAll();
		ApplicationContext ac = new AnnotationConfigApplicationContext(ConfigRequestPlus.class);
		// the Config class has been modified, these need to be loaded again
		CouchbaseTemplate couchbaseTemplateRP = (CouchbaseTemplate) ac.getBean(COUCHBASE_TEMPLATE);
		AirportRepository airportRepositoryRP = (AirportRepository) ac.getBean("airportRepository");

		// save() followed by query with NOT_BOUNDED will result in not finding the document
		Airport vie = new Airport("airports::vie", "vie", "low9");
		Airport airport2 = null;
		for (int i = 1; i <= 100; i++) {
			// set version == 0 so save() will be an upsert, not a replace
			Airport saved = airportRepositoryRP.save(vie.clearVersion());
			try {
				airport2 = airportRepositoryRP.iata(saved.getIata());
				if (airport2 == null) {
					break;
				}
			} catch (DataRetrievalFailureException drfe) {
				airport2 = null; //
			} finally {
				// airportRepository.delete(vie);
				// instead of delete, use removeResult to test QueryOptions.consistentWith()
				RemoveResult removeResult = couchbaseTemplateRP.removeById().one(vie.getId());
				assertEquals(vie.getId(), removeResult.getId());
				assertTrue(removeResult.getCas() != 0);
				assertTrue(removeResult.getMutationToken().isPresent());
				Airport airport3 = airportRepositoryRP.iata(vie.getIata());
				assertNull(airport3, "should have been removed");
			}
		}
		assertNotNull(airport2, "airport2 should have never been null");
		Airport saved = airportRepositoryRP.save(vie.clearVersion());
		List<Airport> airports = couchbaseTemplateRP.findByQuery(Airport.class).withConsistency(NOT_BOUNDED).all();
		RemoveResult removeResult = couchbaseTemplateRP.removeById().one(saved.getId());
		assertFalse(!airports.isEmpty(), "airports should have been empty");
	}

	@Test
	public void saveNotBoundedWithDefaultRepository() {
		airportRepository.withOptions(QueryOptions.queryOptions().scanConsistency(REQUEST_PLUS)).deleteAll();
		ApplicationContext ac = new AnnotationConfigApplicationContext(Config.class);
		// the Config class has been modified, these need to be loaded again
		CouchbaseTemplate couchbaseTemplateRP = (CouchbaseTemplate) ac.getBean(COUCHBASE_TEMPLATE);
		AirportRepositoryScanConsistencyTest airportRepositoryRP = (AirportRepositoryScanConsistencyTest) ac
				.getBean("airportRepositoryScanConsistencyTest");

		List<Airport> sizeBeforeTest = airportRepositoryRP.findAll();
		assertEquals(0, sizeBeforeTest.size());

		Airport vie = new Airport("airports::vie", "vie", "low9");
		Airport saved = airportRepositoryRP.save(vie);
		List<Airport> allSaved = airportRepositoryRP.findAll();
		couchbaseTemplate.removeById(Airport.class).one(saved.getId());
		assertNotEquals(1, allSaved.size(), "should not have found 1 airport");
	}

	@Test
	public void saveRequestPlusWithDefaultRepository() {

		ApplicationContext ac = new AnnotationConfigApplicationContext(ConfigRequestPlus.class);
		// the Config class has been modified, these need to be loaded again
		AirportRepositoryScanConsistencyTest airportRepositoryRP = (AirportRepositoryScanConsistencyTest) ac
				.getBean("airportRepositoryScanConsistencyTest");

		List<Airport> sizeBeforeTest = airportRepositoryRP.findAll();
		assertEquals(0, sizeBeforeTest.size());

		Airport vie = new Airport("airports::vie", "vie", "low9");
		Airport saved = airportRepositoryRP.save(vie);
		List<Airport> allSaved = airportRepositoryRP.findAll();
		couchbaseTemplate.removeById(Airport.class).one(saved.getId());
		assertEquals(1, allSaved.size(), "should have found 1 airport");
	}

	@Test
	void findByTypeAlias() {
		Airport vie = null;
		try {
			vie = new Airport("airports::vie", "vie", "loww");
			vie = airportRepository.save(vie);
			List<Airport> airports = couchbaseTemplate.findByQuery(Airport.class)
					.withConsistency(QueryScanConsistency.REQUEST_PLUS)
					.matching(org.springframework.data.couchbase.core.query.Query
							.query(QueryCriteria.where(N1QLExpression.x("_class")).is("airport")))
					.all();
			assertFalse(airports.isEmpty(), "should have found aiport");
		} finally {
			airportRepository.delete(vie);
		}
	}

	@Test
	void findByEnum() {
		Airport vie = null;
		Airport zzz = null;
		try {
			vie = new Airport("airports::vie", "vie", "loww");
			vie = airportRepository.save(vie);
			zzz = airportRepository.save(vie.withId("airports::zzz").withIata("zzz"));
			Airport airport2 = airportRepository.findByIata(Iata.vie);
			assertNotNull(airport2, "should have found " + vie);
			assertEquals(airport2.getId(), vie.getId());
			Airport airport3 = airportRepository.findByIataIn(new Iata[] { Iata.vie, Iata.xxx });
			assertNotNull(airport3, "should have found " + vie);
			assertEquals(airport3.getId(), vie.getId());

			java.util.Collection<Iata> iatas = new ArrayList<>();
			iatas.add(Iata.vie);
			iatas.add(Iata.xxx);
			Airport airport4 = airportRepository.findByIataIn(iatas);
			assertNotNull(airport4, "should have found " + vie);
			assertEquals(airport4.getId(), vie.getId());

			Airport airport5 = airportRepository.findByIataIn(Iata.vie, Iata.xxx);
			assertNotNull(airport5, "should have found " + vie);
			assertEquals(airport5.getId(), vie.getId());

			JsonArray iatasJson = JsonArray.ja();
			iatasJson.add(Iata.vie.toString());
			iatasJson.add(Iata.xxx.toString());
			Airport airport6 = airportRepository.findByIataIn(iatasJson);
			assertNotNull(airport6, "should have found " + vie);
			assertEquals(airport6.getId(), vie.getId());
		} finally {
			airportRepository.delete(vie);
			airportRepository.delete(zzz);
		}
	}

	/**
	 * can test against _default._default without setting up additional scope/collection and also test for collections and
	 * scopes that do not exist These same tests should be repeated on non-default scope and collection in a test that
	 * supports collections
	 */
	@Test
	@IgnoreWhen(missesCapabilities = { Capabilities.QUERY, Capabilities.COLLECTIONS }, clusterTypes = ClusterType.MOCKED)
	void findBySimplePropertyWithCollection() {

		Airport vie = new Airport("airports::vie", "vie", "low7");
		try {
			Airport saved = airportRepository.withScope(scopeName).withCollection(collectionName).save(vie);
			// given collection (on scope used by template)
			Airport airport2 = airportRepository.withCollection(collectionName)
					.withOptions(QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS))
					.iata(vie.getIata());
			assertEquals(saved, airport2);

			// given scope and collection

			Airport airport3 = airportRepository.withScope(scopeName).withCollection(collectionName)
					.withOptions(QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS))
					.iata(vie.getIata());
			assertEquals(saved, airport3);

			// given bad collection
			assertThrows(IndexFailureException.class,
					() -> airportRepository.withCollection("bogusCollection").iata(vie.getIata()));

			// given bad scope
			assertThrows(IndexFailureException.class, () -> airportRepository.withScope("bogusScope").iata(vie.getIata()));

		} finally {
			airportRepository.delete(vie);
		}
	}

	@Test
	@IgnoreWhen(hasCapabilities = { Capabilities.COLLECTIONS }, clusterTypes = ClusterType.MOCKED)
	void findBySimplePropertyWithCollectionFail() {
		// can test against _default._default without setting up additional scope/collection
		// the server will throw an exception if it doesn't support COLLECTIONS
		Airport vie = new Airport("airports::vie", "vie", "low8");
		try {

			Airport saved = airportRepository.save(vie);

			assertThrows(CouchbaseException.class, () -> airportRepository.withScope("non_default_scope_name")
					.withCollection(collectionName).iata(vie.getIata()));

		} finally {
			airportRepository.delete(vie);
		}
	}

	@Test
	void findBySimplePropertyWithOptions() {

		Airport vie = new Airport("airports::vie", "vie", "low9");
		JsonArray positionalParams = JsonArray.create().add("this parameter will be overridden");
		// JsonObject namedParams = JsonObject.create().put("$1", vie.getIata());
		try {
			Airport saved = airportRepository.save(vie);
			// Duration of 1 nano-second will cause timeout
			assertThrows(AmbiguousTimeoutException.class, () -> airportRepository
					.withOptions(QueryOptions.queryOptions().timeout(Duration.ofNanos(1))).iata(vie.getIata()));

			Airport airport3 = airportRepository.withOptions(
					QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS).parameters(positionalParams))
					.iata(vie.getIata());
			assertEquals(saved, airport3);

		} finally {
			airportRepository.delete(vie);
		}

	}

	@Test
	public void saveNotBounded() {
		// save() followed by query with NOT_BOUNDED will result in not finding the document
		Airport vie = new Airport("airports::vie", "vie", "low9");
		Airport airport2 = null;
		for (int i = 1; i <= 100; i++) {
			// set version == 0 so save() will be an upsert, not a replace
			Airport saved = airportRepository.save(vie.clearVersion());
			try {
				airport2 = airportRepository.iata(saved.getIata());
				if (airport2 == null) {
					break;
				}
			} catch (DataRetrievalFailureException drfe) {
				airport2 = null; //
			} finally {
				// airportRepository.delete(vie);
				// instead of delete, use removeResult to test QueryOptions.consistentWith()
				RemoveResult removeResult = couchbaseTemplate.removeById().one(vie.getId());
				assertEquals(vie.getId(), removeResult.getId());
				assertTrue(removeResult.getCas() != 0);
				assertTrue(removeResult.getMutationToken().isPresent());
				Airport airport3 = airportRepository.iata(vie.getIata());
				assertNull(airport3, "should have been removed");
			}
		}
		assertNull(airport2, "airport2 should have likely been null at least once");
		Airport saved = airportRepository.save(vie.clearVersion());
		couchbaseTemplate.findByQuery(Airport.class).withConsistency(REQUEST_PLUS).all();
		airport2 = airportRepository.iata(vie.getIata());
		RemoveResult removeResult = couchbaseTemplate.removeById().one(saved.getId());
		assertNotNull(airport2, "airport2 should have been found");
	}

	@Test
	public void testTransient() {
		User user = new User("1", "Dave", "Wilson");
		user.setTransientInfo("something");
		userRepository.save(user);
		Optional<User> foundUser = userRepository.findById(user.getId());
		assertEquals(null, foundUser.get().getTransientInfo());
		userRepository.delete(user);
	}

	@Test
	public void testCas() {
		User user = new User("1", "Dave", "Wilson");
		userRepository.save(user);
		long saveVersion = user.getVersion();
		user.setVersion(user.getVersion() - 1);
		assertThrows(OptimisticLockingFailureException.class, () -> userRepository.save(user));
		user.setVersion(saveVersion);
		userRepository.save(user);
		userRepository.delete(user);
	}

	@Test
	public void testExpiration() {
		Airport airport = new Airport("1", "iata21", "icao21");
		airportRepository.withOptions(InsertOptions.insertOptions().expiry(Duration.ofSeconds(10))).save(airport);
		Airport foundAirport = airportRepository.findByIata(airport.getIata());
		assertNotEquals(0, foundAirport.getExpiration());
		airportRepository.delete(airport);
	}

	@Test
	public void testStreamQuery() {
		User user1 = new User("1", "Dave", "Wilson");
		User user2 = new User("2", "Brian", "Wilson");

		userRepository.save(user1);
		userRepository.save(user2);
		List<User> users = userRepository.findByLastname("Wilson").collect(Collectors.toList());
		assertEquals(2, users.size());
		assertTrue(users.contains(user1));
		assertTrue(users.contains(user2));
		userRepository.delete(user1);
		userRepository.delete(user2);
	}

	@Test
	public void testExpiryAnnotation() {
		UserAnnotated user = new UserAnnotated("1", "Dave", "Wilson");
		userRepository.save(user);
		userRepository.findByFirstname("Dave");
		sleep(2000);
		assertThrows(DataRetrievalFailureException.class, () -> userRepository.delete(user));
	}

	@Test
	void stringQueryReturnsSimpleType() {
		Airport airport1 = new Airport("1", "myIata1", "MyIcao");
		airportRepository.save(airport1);
		Airport airport2 = new Airport("2", "myIata2__", "MyIcao");
		airportRepository.save(airport2);
		List<String> iatas = airportRepository.getStrings();
		assertEquals(Arrays.asList(airport1.getIata(), airport2.getIata()), iatas);
		List<Long> iataLengths = airportRepository.getLongs();
		assertEquals(Arrays.asList(airport1.getIata().length(), airport2.getIata().length()).toString(),
				iataLengths.toString());
		// this is somewhat broken, because decode is told that each "row" is just a String instead of a String[]
		// As such, only the first element is returned. (QueryExecutionConverts.unwrapWrapperTypes)
		List<String[]> iataAndIcaos = airportRepository.getStringArrays();
		assertEquals(airport1.getIata(), iataAndIcaos.get(0)[0]);
		assertEquals(airport2.getIata(), iataAndIcaos.get(1)[0]);
		airportRepository.deleteById(airport1.getId());
		airportRepository.deleteById(airport2.getId());
	}

	@Test
	void sortedRepository() {
		airportRepository.withOptions(QueryOptions.queryOptions().scanConsistency(REQUEST_PLUS)).deleteAll();
		String[] iatas = { "JFK", "IAD", "SFO", "SJC", "SEA", "LAX", "PHX" };

		try {
			airportRepository.saveAll(
					Arrays.stream(iatas).map((iata) -> new Airport("airports::" + iata, iata, iata.toLowerCase(Locale.ROOT)))
							.collect(Collectors.toSet()));
			List<Airport> airports = airportRepository.withOptions(QueryOptions.queryOptions().scanConsistency(REQUEST_PLUS))
					.findAll(Sort.by("iata"));
			String[] sortedIatas = iatas.clone();
			System.out.println("" + iatas.length + " " + sortedIatas.length);
			Arrays.sort(sortedIatas);
			for (int i = 0; i < sortedIatas.length; i++) {
				assertEquals(sortedIatas[i], airports.get(i).getIata());
			}
		} finally {
			airportRepository
					.deleteAllById(Arrays.stream(iatas).map((iata) -> "airports::" + iata).collect(Collectors.toSet()));
		}
	}

	@Test
	void countSlicePage() {
		airportRepository.withOptions(QueryOptions.queryOptions().scanConsistency(REQUEST_PLUS)).deleteAll();
		String[] iatas = { "JFK", "IAD", "SFO", "SJC", "SEA", "LAX", "PHX" };

		airportRepository.countOne();
		try {
			airportRepository.saveAll(
					Arrays.stream(iatas).map((iata) -> new Airport("airports::" + iata, iata, iata.toLowerCase(Locale.ROOT)))
							.collect(Collectors.toSet()));
			couchbaseTemplate.findByQuery(Airport.class).withConsistency(REQUEST_PLUS).all();
			Long count = airportRepository.countFancyExpression(asList("JFK"), asList("jfk"), false);
			assertEquals(1, count);

			Pageable sPageable = PageRequest.of(0, 2).withSort(Sort.by("iata"));
			Page<Airport> sPage = airportRepository.getAllByIataNot("JFK", sPageable);
			assertEquals(iatas.length - 1, sPage.getTotalElements());
			assertEquals(sPageable.getPageSize(), sPage.getContent().size());

			Pageable pageable = PageRequest.of(0, 2).withSort(Sort.by("iata"));
			Page<Airport> aPage = airportRepository.findAllByIataNot("JFK", pageable);
			assertEquals(iatas.length - 1, aPage.getTotalElements());
			assertEquals(pageable.getPageSize(), aPage.getContent().size());

			long airportCount = airportRepository.count();
			assertEquals(7, airportCount);

			airportCount = airportRepository.countByIataIn("JFK", "IAD", "SFO");
			assertEquals(3, airportCount);

			airportCount = airportRepository.countByIcaoAndIataIn("jfk", "JFK", "IAD", "SFO", "XXX");
			assertEquals(1, airportCount);

			airportCount = airportRepository.countByIcaoOrIataIn("jfk", "LAX", "IAD", "SFO");
			assertEquals(4, airportCount);

			airportCount = airportRepository.countByIataIn("XXX");
			assertEquals(0, airportCount);

			pageable = PageRequest.of(1, 2, Sort.by("iata"));
			Slice<Airport> airportSlice = airportRepository.fetchSlice("AAA", "zzz", pageable);
			assertEquals(2, airportSlice.getSize());
			assertEquals("LAX", airportSlice.getContent().get(0).getIata());
			assertEquals("PHX", airportSlice.getContent().get(1).getIata());

			pageable = PageRequest.of(1, 2, Sort.by("iata"));
			Page<Airport> airportPage = airportRepository.fetchPage("AAA", "zzz", pageable);
			assertEquals(2, airportPage.getSize());
			assertEquals("LAX", airportPage.getContent().get(0).getIata());
			assertEquals("PHX", airportPage.getContent().get(1).getIata());

		} finally {
			airportRepository
					.deleteAllById(Arrays.stream(iatas).map((iata) -> "airports::" + iata).collect(Collectors.toSet()));
		}
	}

	@Test
	void badCount() {
		assertThrows(CouchbaseQueryExecutionException.class, () -> airportRepository.countBad());
	}

	@Test
	void goodCount() {
		airportRepository.countGood();
	}

	@Test
	void threadSafeParametersTest() throws Exception {
		String[] iatas = { "JFK", "IAD", "SFO", "SJC", "SEA", "LAX", "PHX" };
		Future[] future = new Future[iatas.length];
		ExecutorService executorService = Executors.newFixedThreadPool(iatas.length);

		try {
			for (int i = 0; i < iatas.length; i++) {
				Airport airport = new Airport("airports::" + iatas[i], iatas[i] /*iata*/,
						iatas[i].toLowerCase(Locale.ROOT) /* lcao */);
				airportRepository.save(airport);
			}

			for (int k = 0; k < 50; k++) {
				Callable<Boolean>[] suppliers = new Callable[iatas.length];
				for (int i = 0; i < iatas.length; i++) {
					final int idx = i;
					suppliers[i] = () -> {
						sleep(iatas.length - idx); // so they are executed out-of-order
						List<Airport> airports = airportRepository.findAllByIata(iatas[idx]);
						String foundName = airportRepository.findAllByIata(iatas[idx]).get(0).getIata();
						assertEquals(iatas[idx], foundName);
						return iatas[idx].equals(foundName);
					};
				}
				for (int i = 0; i < iatas.length; i++) {
					future[i] = executorService.submit(suppliers[i]);
				}
				for (int i = 0; i < iatas.length; i++) {
					future[i].get(); // check is done in Callable
				}
			}

		} finally {
			executorService.shutdown();
			for (int i = 0; i < iatas.length; i++) {
				Airport airport = new Airport("airports::" + iatas[i], iatas[i] /*iata*/, iatas[i] /* lcao */);
				airportRepository.delete(airport);
			}
		}
	}

	@Test
	void distinct() {
		String[] iatas = { "JFK", "IAD", "SFO", "SJC", "SEA", "LAX", "PHX" };
		String[] icaos = { "ic0", "ic1", "ic0", "ic1", "ic0", "ic1", "ic0" };
		airportRepository.withOptions(QueryOptions.queryOptions().scanConsistency(REQUEST_PLUS)).deleteAll();

		try {
			for (int i = 0; i < iatas.length; i++) {
				Airport airport = new Airport("airports::" + iatas[i], iatas[i] /*iata*/, icaos[i] /* icao */);
				couchbaseTemplate.insertById(Airport.class).one(airport);
			}

			// distinct icao - parser requires 'By' on the end or it does not match pattern.
			List<Airport> airports1 = airportRepository.findDistinctIcaoBy();
			assertEquals(2, airports1.size());

			List<Airport> airports2 = airportRepository.findDistinctIcaoAndIataBy();
			assertEquals(7, airports2.size());

			// count( distinct { iata, icao } )
			long count1 = airportRepository.countDistinctIcaoAndIataBy();
			assertEquals(7, count1);

			// count( distinct { icao } )
			long count2 = airportRepository.countDistinctIcaoBy();
			assertEquals(2, count2);

		} finally {
			couchbaseTemplate.removeById()
					.all(Arrays.stream(iatas).map((iata) -> "airports::" + iata).collect(Collectors.toSet()));
		}
	}

	@Test
	void stringQueryTest() throws Exception {
		Airport airport = new Airport("airports::vie", "vie", "lowx");
		try {
			airportRepository.save(airport);
			airportRepository.getAllByIata("vie").get(0); // gets at least one with no exception
			assertThrows(CouchbaseException.class, () -> airportRepository.getAllByIataNoID("vie"));
			assertThrows(CouchbaseException.class, () -> airportRepository.getAllByIataNoCAS("vie"));
		} finally {
			airportRepository.deleteById(airport.getId());
		}
	}

	@Test
	void stringDeleteTest() throws Exception {
		Airport airport = new Airport("airports::vie", "vie", "lowx");
		Airport otherAirport = new Airport("airports::xxx", "xxx", "lxxx");
		try {
			airportRepository.save(airport);
			airportRepository.save(otherAirport);
			assertEquals(1, airportRepository.deleteByIata("vie").size()); // gets exactly one with no exception
		} finally {
			airportRepository.deleteById(otherAirport.getId());
		}
	}

	@Test
	void threadSafeStringParametersTest() throws Exception {
		String[] iatas = { "JFK", "IAD", "SFO", "SJC", "SEA", "LAX", "PHX" };
		Future[] future = new Future[iatas.length];
		ExecutorService executorService = Executors.newFixedThreadPool(iatas.length);

		try {
			for (int i = 0; i < iatas.length; i++) {
				Airport airport = new Airport("airports::" + iatas[i], iatas[i] /*iata*/, iatas[i].toLowerCase() /* lcao */);
				airportRepository.save(airport);
			}

			for (int k = 0; k < 100; k++) {
				Callable<Boolean>[] suppliers = new Callable[iatas.length];
				for (int i = 0; i < iatas.length; i++) {
					final int idx = i;
					suppliers[i] = () -> {
						sleep(iatas.length - idx); // so they are executed out-of-order
						String foundName = airportRepository.getAllByIata(iatas[idx]).get(0).getIata();
						assertEquals(iatas[idx], foundName);
						return iatas[idx].equals(foundName);
					};
				}
				for (int i = 0; i < iatas.length; i++) {
					future[i] = executorService.submit(suppliers[i]);
				}
				for (int i = 0; i < iatas.length; i++) {
					future[i].get(); // check is done in Callable
				}
			}
		} finally {
			executorService.shutdown();
			for (int i = 0; i < iatas.length; i++) {
				Airport airport = new Airport("airports::" + iatas[i], iatas[i] /*iata*/, iatas[i] /* lcao */);
				airportRepository.delete(airport);
			}
		}
	}

	@Test
	// DATACOUCH-650
	void deleteAllById() {

		Airport vienna = new Airport("airports::vie", "vie", "LOWW");
		Airport frankfurt = new Airport("airports::fra", "fra", "EDDF");
		Airport losAngeles = new Airport("airports::lax", "lax", "KLAX");
		try {
			airportRepository.saveAll(asList(vienna, frankfurt, losAngeles));
			airportRepository.deleteAllById(asList(vienna.getId(), losAngeles.getId()));
			assertThat(airportRepository.findAll()).containsExactly(frankfurt);
		} finally {
			airportRepository.deleteAll();
		}
	}

	@Test
	void couchbaseRepositoryQuery() throws Exception {
		User user = new User("1", "Dave", "Wilson");
		userRepository.save(user);
		couchbaseTemplate.findByQuery(User.class).withConsistency(REQUEST_PLUS)
				.matching(QueryCriteria.where("firstname").is("Dave").and("`1`").is("`1`")).all();
		String input = "findByFirstname";
		Method method = UserRepository.class.getMethod(input, String.class);
		CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method,
				new DefaultRepositoryMetadata(UserRepository.class), new SpelAwareProxyProjectionFactory(),
				couchbaseTemplate.getConverter().getMappingContext());
		CouchbaseRepositoryQuery query = new CouchbaseRepositoryQuery(couchbaseTemplate, queryMethod, null);
		List<User> users = (List<User>) query.execute(new String[] { "Dave" });
		assertEquals(user, users.get(0));
	}

	@Test
	void findBySimplePropertyAudited() {
		Airport vie = null;
		try {
			vie = new Airport("airports::vie", "vie", "low2");
			Airport saved = airportRepository.save(vie);
			List<Airport> airports1 = airportRepository.findAllByIata("vie");
			assertEquals(saved, airports1.get(0));
			assertEquals(saved.getCreatedBy(), NaiveAuditorAware.AUDITOR); // NaiveAuditorAware will provide this
		} finally {
			airportRepository.delete(vie);
		}
	}

	@Test
	void findPlusN1qlJoin() throws Exception {

		UserSubmission user = new UserSubmission();
		user.setId(UUID.randomUUID().toString());
		user.setUsername("dave");
		user = couchbaseTemplate.insertById(UserSubmission.class).one(user);

		Address address1 = new Address();
		address1.setId(UUID.randomUUID().toString());
		address1.setStreet("3250 Olcott Street");
		address1.setParentId(user.getId());
		Address address2 = new Address();
		address2.setId(UUID.randomUUID().toString());
		address2.setStreet("148 Castro Street");
		address2.setParentId(user.getId());
		Address address3 = new Address();
		address3.setId(UUID.randomUUID().toString());
		address3.setStreet("123 Sesame Street");
		address3.setParentId(UUID.randomUUID().toString()); // does not belong to user
		address1 = couchbaseTemplate.insertById(Address.class).one(address1);
		address2 = couchbaseTemplate.insertById(Address.class).one(address2);
		address3 = couchbaseTemplate.insertById(Address.class).one(address3);

		List<UserSubmission> users = userSubmissionRepository.findByUsername(user.getUsername());
		assertEquals(2, users.get(0).getOtherAddresses().size());
		for (Address a : users.get(0).getOtherAddresses()) {
			if (!(a.getStreet().equals(address1.getStreet()) || a.getStreet().equals(address2.getStreet()))) {
				throw new Exception("street does not match : " + a);
			}
		}

		UserSubmission foundUser = userSubmissionRepository.findById(user.getId()).get();
		assertEquals(2, foundUser.getOtherAddresses().size());
		for (Address a : foundUser.getOtherAddresses()) {
			if (!(a.getStreet().equals(address1.getStreet()) || a.getStreet().equals(address2.getStreet()))) {
				throw new Exception("street does not match : " + a);
			}
		}

		couchbaseTemplate.removeById(Address.class)
				.all(Arrays.asList(address1.getId(), address2.getId(), address3.getId()));
		couchbaseTemplate.removeById(UserSubmission.class).one(user.getId());
	}

	@Test
	void findByKey() {
		Airport airport = new Airport(UUID.randomUUID().toString(), "iata1038", "icao");
		airportRepository.save(airport);
		Airport found = airportRepository.findByKey(airport.getId());
		assertEquals(airport, found);
		airportRepository.delete(airport);
	}

	private void sleep(int millis) {
		try {
			Thread.sleep(millis); // so they are executed out-of-order
		} catch (InterruptedException ie) {
			;
		}
	}

	@Configuration
	@EnableCouchbaseRepositories("org.springframework.data.couchbase")
	@EnableCouchbaseAuditing(dateTimeProviderRef = "dateTimeProviderRef")
	static class Config extends AbstractCouchbaseConfiguration {

		@Override
		public String getConnectionString() {
			return connectionString();
		}

		@Override
		public String getUserName() {
			return config().adminUsername();
		}

		@Override
		public String getPassword() {
			return config().adminPassword();
		}

		@Override
		public String getBucketName() {
			return bucketName();
		}

		@Bean(name = "auditorAwareRef")
		public NaiveAuditorAware testAuditorAware() {
			return new NaiveAuditorAware();
		}

		@Override
		public void configureEnvironment(final ClusterEnvironment.Builder builder) {
			builder.ioConfig().maxHttpConnections(11).idleHttpConnectionTimeout(Duration.ofSeconds(4));
			return;
		}

		@Bean(name = "dateTimeProviderRef")
		public DateTimeProvider testDateTimeProvider() {
			return new AuditingDateTimeProvider();
		}

		@Bean
		public LocalValidatorFactoryBean validator() {
			return new LocalValidatorFactoryBean();
		}

		@Bean
		public ValidatingCouchbaseEventListener validationEventListener() {
			return new ValidatingCouchbaseEventListener(validator());
		}
	}

	@Configuration
	@EnableCouchbaseRepositories("org.springframework.data.couchbase")
	@EnableCouchbaseAuditing(auditorAwareRef = "auditorAwareRef", dateTimeProviderRef = "dateTimeProviderRef")
	static class ConfigRequestPlus extends AbstractCouchbaseConfiguration {

		@Override
		public String getConnectionString() {
			return connectionString();
		}

		@Override
		public String getUserName() {
			return config().adminUsername();
		}

		@Override
		public String getPassword() {
			return config().adminPassword();
		}

		@Override
		public String getBucketName() {
			return bucketName();
		}

		@Bean(name = "auditorAwareRef")
		public NaiveAuditorAware testAuditorAware() {
			return new NaiveAuditorAware();
		}

		@Bean(name = "dateTimeProviderRef")
		public DateTimeProvider testDateTimeProvider() {
			return new AuditingDateTimeProvider();
		}

		@Override
		public QueryScanConsistency getDefaultConsistency() {
			return REQUEST_PLUS;
		}
	}
}
