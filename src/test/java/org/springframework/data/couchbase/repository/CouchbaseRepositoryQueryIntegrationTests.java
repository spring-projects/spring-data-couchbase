/*
 * Copyright 2017-2021 the original author or authors.
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.data.couchbase.config.BeanNames.COUCHBASE_TEMPLATE;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.domain.*;
import org.springframework.data.couchbase.domain.time.AuditingDateTimeProvider;
import org.springframework.data.couchbase.repository.auditing.EnableCouchbaseAuditing;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.query.CouchbaseQueryMethod;
import org.springframework.data.couchbase.repository.query.CouchbaseRepositoryQuery;
import org.springframework.data.couchbase.repository.support.SimpleCouchbaseRepository;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * Repository tests
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Jens Schauder
 * @author Jonathan Massuchetti
 */
@SpringJUnitConfig(CouchbaseRepositoryQueryIntegrationTests.Config.class)
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
public class CouchbaseRepositoryQueryIntegrationTests extends ClusterAwareIntegrationTests {

	@Autowired CouchbaseClientFactory clientFactory;

	@Autowired AirportRepository airportRepository;

	@Autowired
	AirportDefaultConsistencyRepository airportDefaultConsistencyRepository;

	@Autowired UserRepository userRepository;

	@Autowired CouchbaseTemplate couchbaseTemplate;

	@BeforeEach
	public void beforeEach() {
		try {
			clientFactory.getCluster().queryIndexes().createPrimaryIndex(bucketName());
		} catch (IndexExistsException ex) {
			// ignore, all good.
		}
	}

	@Test
	void shouldSaveAndFindAll() {
		Airport vie = null;
		try {
			vie = new Airport("airports::vie", "vie", "low4");
			airportRepository.save(vie);
			List<Airport> all = new ArrayList<>();
			airportRepository.findAll().forEach(all::add);
			assertFalse(all.isEmpty());
			assertTrue(all.stream().anyMatch(a -> a.getId().equals("airports::vie")));
		} finally {
			airportRepository.delete(vie);
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
	void findBySimpleProperty() {
		Airport vie = null;
		try {
			vie = new Airport("airports::vie", "vie", "low6");
			vie = airportRepository.save(vie);
			List<Airport> airports = airportRepository.findAllByIata("vie");
			assertEquals(1, airports.size());
			Airport airport1 = airportRepository.findById(airports.get(0).getId()).get();
			assertEquals(airport1.getIata(), vie.getIata());
			Airport airport2 = airportRepository.findByIata(airports.get(0).getIata());
			assertEquals(airport1.getId(), vie.getId());
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
			Airport saved = airportDefaultConsistencyRepository.save(vie.clearVersion());
			try {
				airport2 = airportDefaultConsistencyRepository.iata(saved.getIata());
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
				Airport airport3 = airportDefaultConsistencyRepository.iata(vie.getIata());
				assertNull(airport3, "should have been removed");
			}
		}
		assertNull(airport2, "airport2 should have likely been null at least once");
		Airport saved = airportDefaultConsistencyRepository.save(vie.clearVersion());
		couchbaseTemplate.findByQuery(Airport.class).withConsistency(REQUEST_PLUS).all();
		airport2 = airportDefaultConsistencyRepository.iata(vie.getIata());
		RemoveResult removeResult = couchbaseTemplate.removeById().one(saved.getId());
		assertNotNull(airport2, "airport2 should have been found");
	}

	@Test
	public void saveNotBoundedRequestPlus() {
		ApplicationContext ac = new AnnotationConfigApplicationContext(ConfigRequestPlus.class);
		// the Config class has been modified, these need to be loaded again
		CouchbaseTemplate couchbaseTemplateRP = (CouchbaseTemplate) ac.getBean(COUCHBASE_TEMPLATE);
		AirportDefaultConsistencyRepository airportRepositoryRP = (AirportDefaultConsistencyRepository) ac.getBean("airportDefaultConsistencyRepository");

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
	public void saveNotBoundedRequestPlusWithDefaultRepository() {
		ApplicationContext ac = new AnnotationConfigApplicationContext(ConfigRequestPlus.class);
		// the Config class has been modified, these need to be loaded again
		CouchbaseTemplate couchbaseTemplateRP = (CouchbaseTemplate) ac.getBean(COUCHBASE_TEMPLATE);
		AirportDefaultConsistencyRepository airportRepositoryRP = (AirportDefaultConsistencyRepository) ac.getBean("airportDefaultConsistencyRepository");

		List<Airport> sizeBeforeTest = airportRepositoryRP.findAll();
		assertEquals(0, sizeBeforeTest.size());

		List<String> idsToRemove = new ArrayList<>(100);
		for (int i = 1; i <= 100; i++) {
			Airport vie = new Airport("airports::vie" + i, "vie" + i, "low9");
			Airport saved = airportRepositoryRP.save(vie);
			idsToRemove.add(saved.getId());
		}

		List<Airport> allSaved = airportRepositoryRP.findAll();

		boolean success = allSaved.size() == 100;

		for (String idToRemove : idsToRemove) {
			couchbaseTemplateRP.removeById().one(idToRemove);
		}

		assertTrue(success);
	}

	@Test
	void findByTypeAlias() {
		Airport vie = null;
		try {
			vie = new Airport("airports::vie", "vie", "loww");
			vie = airportRepository.save(vie);
			List<Airport> airports = couchbaseTemplate.findByQuery(Airport.class).withConsistency(REQUEST_PLUS)
					.matching(new Query(QueryCriteria.where(N1QLExpression.x("_class")).is("airport"))).all();
			assertFalse(airports.isEmpty(), "should have found aiport");
		} finally {
			airportRepository.delete(vie);
		}
	}

	@Test
	void findByEnum() {
		Airport vie = null;
		try {
			vie = new Airport("airports::vie", "vie", "loww");
			vie = airportRepository.save(vie);
			Airport airport2 = airportRepository.findByIata(Iata.vie);
			assertNotNull(airport2, "should have found " + vie);
			assertEquals(airport2.getId(), vie.getId());
		} finally {
			airportRepository.delete(vie);
		}
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
		user.setVersion(user.getVersion() - 1);
		assertThrows(DataIntegrityViolationException.class, () -> userRepository.save(user));
		user.setVersion(0);
		userRepository.save(user);
		userRepository.delete(user);
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
	void count() {
		String[] iatas = { "JFK", "IAD", "SFO", "SJC", "SEA", "LAX", "PHX" };

		try {

			airportRepository.saveAll(
					Arrays.stream(iatas).map((iata) -> new Airport("airports::" + iata, iata, iata.toLowerCase(Locale.ROOT)))
							.collect(Collectors.toSet()));
			couchbaseTemplate.findByQuery(Airport.class).withConsistency(REQUEST_PLUS).all();
			Long count = airportRepository.countFancyExpression(asList("JFK"), asList("jfk"), false);
			assertEquals(1, count);

			Pageable pageable = PageRequest.of(0, 2);
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

		} finally {
			airportRepository
					.deleteAllById(Arrays.stream(iatas).map((iata) -> "airports::" + iata).collect(Collectors.toSet()));
		}
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

	@Test // DATACOUCH-650
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

	private void sleep(int millis) {
		try {
			Thread.sleep(millis); // so they are executed out-of-order
		} catch (InterruptedException ie) {}
	}

	@Configuration
	@EnableCouchbaseRepositories("org.springframework.data.couchbase")
	@EnableCouchbaseAuditing(auditorAwareRef = "auditorAwareRef", dateTimeProviderRef = "dateTimeProviderRef")
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

		@Bean(name = "dateTimeProviderRef")
		public DateTimeProvider testDateTimeProvider() {
			return new AuditingDateTimeProvider();
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
