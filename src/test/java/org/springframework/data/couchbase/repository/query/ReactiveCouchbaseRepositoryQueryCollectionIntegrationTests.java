/*
 * Copyright 2017-present the original author or authors.
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
package org.springframework.data.couchbase.repository.query;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.springframework.data.couchbase.domain.AirportRepository;
import reactor.core.Disposable;

import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.domain.Airport;
import org.springframework.data.couchbase.domain.ConfigScoped;
import org.springframework.data.couchbase.domain.ReactiveAirportMustScopeRepository;
import org.springframework.data.couchbase.domain.ReactiveAirportRepository;
import org.springframework.data.couchbase.domain.ReactiveAirportRepositoryAnnotated;
import org.springframework.data.couchbase.domain.ReactiveUserColRepository;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserCol;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.CollectionAwareIntegrationTests;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.core.error.IndexFailureException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * Reactive Repository Query Tests with Collections
 *
 * @author Michael Reiche
 */
@SpringJUnitConfig(ConfigScoped.class)
@IgnoreWhen(missesCapabilities = { Capabilities.QUERY, Capabilities.COLLECTIONS }, clusterTypes = ClusterType.MOCKED)
@DirtiesContext
public class ReactiveCouchbaseRepositoryQueryCollectionIntegrationTests extends CollectionAwareIntegrationTests {

	@Autowired ReactiveAirportRepository reactiveAirportRepository;
	@Autowired ReactiveAirportRepositoryAnnotated reactiveAirportRepositoryAnnotated;
    @Autowired ReactiveAirportMustScopeRepository reactiveAirportMustScopeRepository;
	@Autowired ReactiveUserColRepository userColRepository;
	@Autowired public CouchbaseTemplate couchbaseTemplate;
	@Autowired public ReactiveCouchbaseTemplate reactiveCouchbaseTemplate;

	@BeforeAll
	public static void beforeAll() {
		// first call the super method
		callSuperBeforeAll(new Object() {});
		// then do processing for this class
	}

	@AfterAll
	public static void afterAll() {
		// first do the processing for this class
		// no-op
		// then call the super method
		callSuperAfterAll(new Object() {});
	}

	@BeforeEach
	@Override
	public void beforeEach() {
		// first call the super method
		super.beforeEach();
		// then do processing for this class
		couchbaseTemplate.removeByQuery(User.class).inCollection(collectionName).all();
		couchbaseTemplate.removeByQuery(UserCol.class).inScope(otherScope).inCollection(otherCollection).all();
	}

	@AfterEach
	@Override
	public void afterEach() {
		// first do processing for this class
		// no-op
		// then call the super method
		super.afterEach();
	}

	@Test
	public void myTest() {

		ReactiveAirportRepository ar = reactiveAirportRepository.withScope(scopeName).withCollection(collectionName);
		Airport vie = new Airport("airports::vie", "vie", "loww");
		try {
			Airport saved = ar.save(vie).block();
			Airport airport2 = ar.save(saved).block();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			ar.delete(vie).block();
		}

	}

  @Test
  void testThreadLocal() throws InterruptedException {

    String scopeName = "my_scope";
    String id = UUID.randomUUID().toString();

    Airport airport = new Airport(id, "testThreadLocal", "icao");
    reactiveAirportMustScopeRepository.withScope(scopeName).findById(airport.getId()).doOnNext(u -> {
        throw new RuntimeException("User already Exists! " + u);
    }).then(reactiveAirportMustScopeRepository.withScope(scopeName).save(airport))
        .block();

    reactiveAirportMustScopeRepository.withScope(scopeName).deleteById(id).block();
    }

	/**
	 * can test against _default._default without setting up additional scope/collection and also test for collections and
	 * scopes that do not exist These same tests should be repeated on non-default scope and collection in a test that
	 * supports collections
	 */
	@Test
	@IgnoreWhen(missesCapabilities = { Capabilities.QUERY, Capabilities.COLLECTIONS }, clusterTypes = ClusterType.MOCKED)
	void findBySimplePropertyWithCollection() {

		Airport vie = new Airport("airports::vie", "vie", "loww");
		// create proxy with scope, collection
		ReactiveAirportRepository ar = reactiveAirportRepository.withScope(scopeName).withCollection(collectionName);
		try {
			Airport saved = ar.save(vie).block();

			// valid scope, collection in options
			Airport airport2 = ar.withOptions(QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS))
					.iata(vie.getIata()).block();
			assertEquals(saved, airport2);

			// given bad collectionName in fluent
			assertThrows(IndexFailureException.class, () -> ar.withCollection("bogusCollection").iata(vie.getIata()).block());

			// given bad scopeName in fluent
			assertThrows(IndexFailureException.class, () -> ar.withScope("bogusScope").iata(vie.getIata()).block());

			Airport airport6 = ar.withOptions(QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS))
					.iata(vie.getIata()).block();
			assertEquals(saved, airport6);

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			ar.deleteAll().block();
		}
	}

	@Test
	void findBySimplePropertyWithOptions() {

		Airport vie = new Airport("airports::vie", "vie", "loww");
		ReactiveAirportRepository ar = reactiveAirportRepository.withScope(scopeName).withCollection(collectionName);
		JsonArray positionalParams = JsonArray.create().add("\"this parameter will be overridden\"");
		try {
			Airport saved = ar.save(vie).block();

			Airport airport3 = ar.withOptions(
					QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS).parameters(positionalParams))
					.iata(vie.getIata()).block();
			assertEquals(saved, airport3);

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			ar.delete(vie).block();
		}
	}

	@Test
	public void testScopeCollectionAnnotation() {
		// template default scope is my_scope
		// UserCol annotation scope is other_scope
		UserCol user = new UserCol("1", "Dave", "Wilson");
		try {
			UserCol saved = userColRepository.withCollection(otherCollection).save(user).block(); // should use UserCol
																																														// annotation
			// scope
			List<UserCol> found = userColRepository.withCollection(otherCollection).findByFirstname(user.getFirstname())
					.collectList().block();
			assertEquals(saved, found.get(0), "should have found what was saved");
			List<UserCol> notfound = userColRepository.withScope(CollectionIdentifier.DEFAULT_SCOPE)
					.withCollection(CollectionIdentifier.DEFAULT_COLLECTION).findByFirstname(user.getFirstname()).collectList()
					.block();
			assertEquals(0, notfound.size(), "should not have found what was saved");
		} finally {
			try {
				userColRepository.withScope(otherScope).withCollection(otherCollection).delete(user);
			} catch (DataRetrievalFailureException drfe) {}
		}
	}

	// template default scope is my_scope
	// UserCol annotation scope is other_scope
	@Test
	public void testScopeCollectionRepoWith() {
		UserCol user = new UserCol("1", "Dave", "Wilson");
		try {
			UserCol saved = userColRepository.withScope(scopeName).withCollection(collectionName).save(user).block();
			List<UserCol> found = userColRepository.withScope(scopeName).withCollection(collectionName)
					.findByFirstname(user.getFirstname()).collectList().block();
			assertEquals(saved, found.get(0), "should have found what was saved");
			List<UserCol> notfound = userColRepository.withScope(CollectionIdentifier.DEFAULT_SCOPE)
					.withCollection(CollectionIdentifier.DEFAULT_COLLECTION).findByFirstname(user.getFirstname()).collectList()
					.block();
			assertEquals(0, notfound.size(), "should not have found what was saved");
			userColRepository.withScope(scopeName).withCollection(collectionName).delete(user).block();
		} finally {
			try {
				userColRepository.withScope(scopeName).withCollection(collectionName).delete(user).block();
			} catch (DataRetrievalFailureException drfe) {}
		}
	}

	@Test
	void stringDeleteCollectionTest() {
		Airport airport = new Airport(loc(), "vie", "abc");
		Airport otherAirport = new Airport(loc(), "xxx", "xyz");
		try {
			airport = reactiveAirportRepository.withScope(scopeName).withCollection(collectionName).save(airport).block();
			otherAirport = reactiveAirportRepository.withScope(scopeName).withCollection(collectionName).save(otherAirport)
					.block();
			assertEquals(1, reactiveAirportRepository.withScope(scopeName).withCollection(collectionName)
					.deleteByIata(airport.getIata()).collectList().block().size());
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			reactiveAirportRepository.withScope(scopeName).withCollection(collectionName).deleteById(otherAirport.getId());
		}
	}

	@Test
	void stringDeleteWithRepositoryAnnotationTest() {
		Airport airport = new Airport(loc(), "vie", "abc");
		Airport otherAirport = new Airport(loc(), "xxx", "xyz");
		try {
			airport = reactiveAirportRepositoryAnnotated.withScope(scopeName).save(airport).block();
			otherAirport = reactiveAirportRepositoryAnnotated.withScope(scopeName).save(otherAirport).block();
			// don't specify a collection - should get collection from AirportRepositoryAnnotated
			assertEquals(1, reactiveAirportRepositoryAnnotated.withScope(scopeName).deleteByIata(airport.getIata())
					.collectList().block().size());
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			// this will fail if the above didn't use collectionName2
			reactiveAirportRepository.withScope(scopeName).withCollection(collectionName2).deleteById(otherAirport.getId());
		}
	}

	@Test
	void stringDeleteWithMethodAnnotationTest() {
		Airport airport = new Airport(loc(), "vie", "abc");
		Airport otherAirport = new Airport(loc(), "xxx", "xyz");
		try {
			Airport airportSaved = reactiveAirportRepositoryAnnotated.withScope(scopeName).save(airport).block();
			Airport otherAirportSaved = reactiveAirportRepositoryAnnotated.withScope(scopeName).save(otherAirport).block();
			// don't specify a collection - should get collection from deleteByIataAnnotated method
			assertThrows(IndexFailureException.class, () -> assertEquals(1, reactiveAirportRepositoryAnnotated
					.withScope(scopeName).deleteByIataAnnotated(airport.getIata()).collectList().block().size()));
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			// this will fail if the above didn't use collectionName2
			reactiveAirportRepository.withScope(scopeName).withCollection(collectionName2).deleteById(otherAirport.getId());
		}
	}

	@Test	// DATACOUCH-650, SDC-1939
	void deleteAllById() {

		Airport vienna = new Airport("airports::vie", "vie", "LOWW");
		Airport frankfurt = new Airport("airports::fra", "fra", "EDDZ");
		Airport losAngeles = new Airport("airports::lax", "lax", "KLAX");
		ReactiveAirportRepository ar = reactiveAirportRepository.withScope(scopeName).withCollection(collectionName);
		try {
			ar.saveAll(asList(vienna, frankfurt, losAngeles)).blockLast();
			List<Airport> airports = ar.findAllById(asList(vienna.getId(), losAngeles.getId())).collectList().block();
			assertEquals(2, airports.size());
			ar.deleteAllById(asList(vienna.getId(), losAngeles.getId())).block();
			assertThat(ar.findAll().collectList().block()).containsExactly(frankfurt);
			ar.deleteAll(asList(frankfurt)).block();
		} finally {
			ar.deleteAll().block();
		}
	}

}
