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
package org.springframework.data.couchbase.repository.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.couchbase.domain.Airport;
import org.springframework.data.couchbase.domain.Config;
import org.springframework.data.couchbase.domain.ReactiveAirportRepository;
import org.springframework.data.couchbase.domain.ReactiveUserColRepository;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserCol;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.CollectionAwareIntegrationTests;
import org.springframework.data.couchbase.util.IgnoreWhen;

import com.couchbase.client.core.error.IndexFailureException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;

@IgnoreWhen(missesCapabilities = { Capabilities.QUERY, Capabilities.COLLECTIONS }, clusterTypes = ClusterType.MOCKED)
public class ReactiveCouchbaseRepositoryQueryCollectionIntegrationTests extends CollectionAwareIntegrationTests {

	@Autowired ReactiveAirportRepository airportRepository;
	@Autowired ReactiveUserColRepository userColRepository;

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

		ApplicationContext ac = new AnnotationConfigApplicationContext(Config.class);
		// seems that @Autowired is not adequate, so ...
		airportRepository = (ReactiveAirportRepository) ac.getBean("reactiveAirportRepository");
		userColRepository = (ReactiveUserColRepository) ac.getBean("reactiveUserColRepository");
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

		Airport vie = new Airport("airports::vie", "vie", "loww");
		try {
			airportRepository = airportRepository.withCollection(collectionName);
			Airport saved = airportRepository.save(vie).block();
			Airport airport2 = airportRepository.save(saved).block();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			airportRepository.delete(vie).block();
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

		Airport vie = new Airport("airports::vie", "vie", "loww");
		// create proxy with scope, collection
		airportRepository = airportRepository.withScope(scopeName).withCollection(collectionName);
		try {
			Airport saved = airportRepository.save(vie).block();

			// valid scope, collection in options
			Airport airport2 = airportRepository.withCollection(collectionName)
					.withOptions(QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS))
					.iata(vie.getIata()).block();
			assertEquals(saved, airport2);

			// given bad collectionName in fluent
			assertThrows(IndexFailureException.class,
					() -> airportRepository.withCollection("bogusCollection").iata(vie.getIata()).block());

			// given bad scopeName in fluent
			assertThrows(IndexFailureException.class,
					() -> airportRepository.withScope("bogusScope").iata(vie.getIata()).block());

			Airport airport6 = airportRepository
					.withOptions(QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS))
					.iata(vie.getIata()).block();
			assertEquals(saved, airport6);

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			airportRepository.withScope(scopeName).withCollection(collectionName).deleteAll().block();
		}
	}

	@Test
	void findBySimplePropertyWithOptions() {

		Airport vie = new Airport("airports::vie", "vie", "loww");
		JsonArray positionalParams = JsonArray.create().add("\"this parameter will be overridden\"");
		try {
			Airport saved = airportRepository.withCollection(collectionName).save(vie).block();

			Airport airport3 = airportRepository
					.withCollection(collectionName).withOptions(QueryOptions.queryOptions()
							.scanConsistency(QueryScanConsistency.REQUEST_PLUS).parameters(positionalParams))
					.iata(vie.getIata()).block();
			assertEquals(saved, airport3);

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			airportRepository.withCollection(collectionName).delete(vie).block();
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
}
