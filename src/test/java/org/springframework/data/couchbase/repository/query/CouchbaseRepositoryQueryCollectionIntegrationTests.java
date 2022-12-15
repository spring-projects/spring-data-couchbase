/*
 * Copyright 2017-2023 the original author or authors.
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

import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE;
import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
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
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.domain.Address;
import org.springframework.data.couchbase.domain.AddressAnnotated;
import org.springframework.data.couchbase.domain.Airport;
import org.springframework.data.couchbase.domain.AirportRepository;
import org.springframework.data.couchbase.domain.AirportRepositoryAnnotated;
import org.springframework.data.couchbase.domain.CollectionsConfig;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserCol;
import org.springframework.data.couchbase.domain.UserColRepository;
import org.springframework.data.couchbase.domain.UserSubmissionAnnotated;
import org.springframework.data.couchbase.domain.UserSubmissionAnnotatedRepository;
import org.springframework.data.couchbase.domain.UserSubmissionUnannotated;
import org.springframework.data.couchbase.domain.UserSubmissionUnannotatedRepository;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.CollectionAwareIntegrationTests;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.core.error.IndexFailureException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.query.QueryOptions;

/**
 * Repository Query Tests with Collections
 *
 * @author Michael Reiche
 */
@IgnoreWhen(missesCapabilities = { Capabilities.QUERY, Capabilities.COLLECTIONS }, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(CollectionsConfig.class)
public class CouchbaseRepositoryQueryCollectionIntegrationTests extends CollectionAwareIntegrationTests {

	@Autowired AirportRepositoryAnnotated airportRepositoryAnnotated;
	@Autowired AirportRepository airportRepository;
	@Autowired UserColRepository userColRepository;
	@Autowired UserSubmissionAnnotatedRepository userSubmissionAnnotatedRepository;
	@Autowired UserSubmissionUnannotatedRepository userSubmissionUnannotatedRepository;

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
		couchbaseTemplate.removeByQuery(Airport.class).inCollection(collectionName).all();
		couchbaseTemplate.removeByQuery(Airport.class).inCollection(collectionName2).all();
		couchbaseTemplate.findByQuery(Airport.class).withConsistency(REQUEST_PLUS).inCollection(collectionName).all();
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
	void findByKey() {
		UserCol userCol = new UserCol("101", "userColFirst", "userColLast");
		userColRepository.save(userCol);
		UserCol found = userColRepository.getById(userCol.getId());
		System.err.println("found: " + found);
		assertEquals(userCol, found);
		userColRepository.delete(found);
	}

	@Test
	public void myTest() {

		AirportRepository ar = airportRepository.withScope(scopeName).withCollection(collectionName);
		Airport vie = new Airport("airports::vie", "vie", "loww");
		try {
			Airport saved = ar.save(vie);
			Airport airport2 = ar.save(saved);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			ar.delete(vie);
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
		AirportRepository ar = airportRepository.withScope(scopeName).withCollection(collectionName);
		try {
			Airport saved = ar.save(vie);

			// valid scope, collection in options
			Airport airport2 = ar.withCollection(collectionName)
					.withOptions(QueryOptions.queryOptions().scanConsistency(REQUEST_PLUS)).iata(vie.getIata());
			assertEquals(saved, airport2);

			// given bad collectionName in fluent
			assertThrows(IndexFailureException.class, () -> ar.withCollection("bogusCollection").iata(vie.getIata()));

			// given bad scopeName in fluent
			assertThrows(IndexFailureException.class, () -> ar.withScope("bogusScope").iata(vie.getIata()));

			Airport airport6 = ar.withOptions(QueryOptions.queryOptions().scanConsistency(REQUEST_PLUS)).iata(vie.getIata());
			assertEquals(saved, airport6);

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			ar.deleteAll();
		}
	}

	@Test
	void findBySimplePropertyWithOptions() {

		AirportRepository ar = airportRepository.withScope(scopeName).withCollection(collectionName);
		Airport vie = new Airport("airports::vie", "vie", "loww");
		JsonArray positionalParams = JsonArray.create().add("\"this parameter will be overridden\"");
		try {
			Airport saved = ar.save(vie);

			Airport airport3 = ar
					.withOptions(QueryOptions.queryOptions().scanConsistency(REQUEST_PLUS).parameters(positionalParams))
					.iata(vie.getIata());
			assertEquals(saved, airport3);

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			ar.delete(vie);
		}
	}

	@Test
	public void testScopeCollectionAnnotation() {
		// template default scope is my_scope
		// UserCol annotation scope is other_scope
		UserCol user = new UserCol("1", "Dave", "Wilson");
		try {
			UserCol saved = userColRepository.withCollection(otherCollection).save(user); // should use UserCol annotation
																																										// scope
			List<UserCol> found = userColRepository.withCollection(otherCollection).findByFirstname(user.getFirstname());
			assertEquals(saved, found.get(0), "should have found what was saved");
			List<UserCol> notfound = userColRepository.withScope(DEFAULT_SCOPE)
					.withCollection(CollectionIdentifier.DEFAULT_COLLECTION).findByFirstname(user.getFirstname());
			assertEquals(0, notfound.size(), "should not have found what was saved "+notfound);
		} finally {
			try {
				userColRepository.withScope(otherScope).withCollection(otherCollection).delete(user);
			} catch (DataRetrievalFailureException drfe) {}
		}
	}

	@Test
	public void testScopeCollectionAnnotationSwap() {
		// UserCol annotation scope is other_scope, collection is other_collection
		// airportRepository relies on Config.setScopeName(scopeName) ("my_scope") from CollectionAwareIntegrationTests.
		// using airportRepository without specified a collection should fail.
		// This test ensures that airportRepository.save(airport) doesn't get the
		// collection from CrudMethodMetadata of UserCol.save()
		UserCol userCol = new UserCol("1", "Dave", "Wilson");
		Airport airport = new Airport("3", "myIata", "myIcao");
		try {
			UserCol savedCol = userColRepository.save(userCol); // uses UserCol annotation scope, populates CrudMethodMetadata
			userColRepository.delete(userCol); // uses UserCol annotation scope, populates CrudMethodMetadata
			assertThrows(IllegalStateException.class, () -> airportRepository.save(airport));
		} finally {
			List<RemoveResult> removed = couchbaseTemplate.removeByQuery(Airport.class).all();
			couchbaseTemplate.findByQuery(Airport.class).withConsistency(REQUEST_PLUS).all();
		}
	}

	// template default scope is my_scope
	// UserCol annotation scope is other_scope
	@Test
	public void testScopeCollectionRepoWith() {
		UserCol user = new UserCol("1", "Dave", "Wilson");
		try {
			UserCol saved = userColRepository.withScope(scopeName).withCollection(collectionName).save(user);
			List<UserCol> found = userColRepository.withScope(scopeName).withCollection(collectionName)
					.findByFirstname(user.getFirstname());
			assertEquals(saved, found.get(0), "should have found what was saved");
			List<UserCol> notfound = userColRepository.withScope(DEFAULT_SCOPE)
					.withCollection(CollectionIdentifier.DEFAULT_COLLECTION).findByFirstname(user.getFirstname());
			assertEquals(0, notfound.size(), "should not have found what was saved "+notfound);
			userColRepository.withScope(scopeName).withCollection(collectionName).delete(user);
		} finally {
			try {
				userColRepository.withScope(scopeName).withCollection(collectionName).delete(user);
			} catch (DataRetrievalFailureException drfe) {}
		}
	}

	@Test
	void findPlusN1qlJoinBothAnnotated() throws Exception {

		// UserSubmissionAnnotated has scope=my_scope, collection=my_collection
		UserSubmissionAnnotated user = new UserSubmissionAnnotated();
		user.setId(UUID.randomUUID().toString());
		user.setUsername("dave");
		user = userSubmissionAnnotatedRepository.save(user);

		// AddressesAnnotated has scope=dummy_scope, collection=my_collection2
		// scope must be explicitly set on template insertById, findByQuery and removeById
		// For userSubmissionAnnotatedRepository.findByUsername(), scope will be taken from UserSubmissionAnnotated
		AddressAnnotated address1 = new AddressAnnotated();
		address1.setId(UUID.randomUUID().toString());
		address1.setStreet("3250 Olcott Street");
		address1.setParentId(user.getId());
		AddressAnnotated address2 = new AddressAnnotated();
		address2.setId(UUID.randomUUID().toString());
		address2.setStreet("148 Castro Street");
		address2.setParentId(user.getId());
		AddressAnnotated address3 = new AddressAnnotated();
		address3.setId(UUID.randomUUID().toString());
		address3.setStreet("123 Sesame Street");
		address3.setParentId(UUID.randomUUID().toString()); // does not belong to user

		try {

			address1 = couchbaseTemplate.insertById(AddressAnnotated.class).inScope(scopeName).one(address1);
			address2 = couchbaseTemplate.insertById(AddressAnnotated.class).inScope(scopeName).one(address2);
			address3 = couchbaseTemplate.insertById(AddressAnnotated.class).inScope(scopeName).one(address3);
			couchbaseTemplate.findByQuery(AddressAnnotated.class).withConsistency(REQUEST_PLUS).inScope(scopeName).all();

			// scope for AddressesAnnotated in N1qlJoin comes from userSubmissionAnnotatedRepository.
			List<UserSubmissionAnnotated> users = userSubmissionAnnotatedRepository.findByUsername(user.getUsername());
			assertEquals(2, users.get(0).getOtherAddresses().size());
			for (Address a : users.get(0).getOtherAddresses()) {
				if (!(a.getStreet().equals(address1.getStreet()) || a.getStreet().equals(address2.getStreet()))) {
					throw new Exception("street does not match : " + a);
				}
			}

			UserSubmissionAnnotated foundUser = userSubmissionAnnotatedRepository.findById(user.getId()).get();
			assertEquals(2, foundUser.getOtherAddresses().size());
			for (Address a : foundUser.getOtherAddresses()) {
				if (!(a.getStreet().equals(address1.getStreet()) || a.getStreet().equals(address2.getStreet()))) {
					throw new Exception("street does not match : " + a);
				}
			}
		} finally {
			couchbaseTemplate.removeById(AddressAnnotated.class).inScope(scopeName)
					.all(Arrays.asList(address1.getId(), address2.getId(), address3.getId()));
			couchbaseTemplate.removeById(UserSubmissionAnnotated.class).one(user.getId());
		}
	}

	@Test
	void findPlusN1qlJoinUnannotated() throws Exception {
		// UserSubmissionAnnotated has scope=my_scope, collection=my_collection
		UserSubmissionUnannotated user = new UserSubmissionUnannotated();
		user.setId(UUID.randomUUID().toString());
		user.setUsername("dave");
		user = userSubmissionUnannotatedRepository.save(user);

		// AddressesAnnotated has scope=dummy_scope, collection=my_collection2
		// scope must be explicitly set on template insertById, findByQuery and removeById
		// For userSubmissionAnnotatedRepository.findByUsername(), scope will be taken from UserSubmissionAnnotated
		AddressAnnotated address1 = new AddressAnnotated();
		address1.setId(UUID.randomUUID().toString());
		address1.setStreet("3250 Olcott Street");
		address1.setParentId(user.getId());
		AddressAnnotated address2 = new AddressAnnotated();
		address2.setId(UUID.randomUUID().toString());
		address2.setStreet("148 Castro Street");
		address2.setParentId(user.getId());
		AddressAnnotated address3 = new AddressAnnotated();
		address3.setId(UUID.randomUUID().toString());
		address3.setStreet("123 Sesame Street");
		address3.setParentId(UUID.randomUUID().toString()); // does not belong to user

		try {

			address1 = couchbaseTemplate.insertById(AddressAnnotated.class).inScope(scopeName).one(address1);
			address2 = couchbaseTemplate.insertById(AddressAnnotated.class).inScope(scopeName).one(address2);
			address3 = couchbaseTemplate.insertById(AddressAnnotated.class).inScope(scopeName).one(address3);
			couchbaseTemplate.findByQuery(AddressAnnotated.class).withConsistency(REQUEST_PLUS).inScope(scopeName).all();

			// scope for AddressesAnnotated in N1qlJoin comes from userSubmissionAnnotatedRepository.
			List<UserSubmissionUnannotated> users = userSubmissionUnannotatedRepository.findByUsername(user.getUsername());
			assertEquals(2, users.get(0).getOtherAddresses().size());
			for (Address a : users.get(0).getOtherAddresses()) {
				if (!(a.getStreet().equals(address1.getStreet()) || a.getStreet().equals(address2.getStreet()))) {
					throw new Exception("street does not match : " + a);
				}
			}

			UserSubmissionUnannotated foundUser = userSubmissionUnannotatedRepository.findById(user.getId()).get();
			assertEquals(2, foundUser.getOtherAddresses().size());
			for (Address a : foundUser.getOtherAddresses()) {
				if (!(a.getStreet().equals(address1.getStreet()) || a.getStreet().equals(address2.getStreet()))) {
					throw new Exception("street does not match : " + a);
				}
			}
		} finally {
			couchbaseTemplate.removeById(AddressAnnotated.class).inScope(scopeName)
					.all(Arrays.asList(address1.getId(), address2.getId(), address3.getId()));
			couchbaseTemplate.removeById(UserSubmissionUnannotated.class).one(user.getId());
		}
	}

	@Test
	void stringDeleteCollectionTest() {
		Airport airport = new Airport(loc(), "vie", "abc");
		Airport otherAirport = new Airport(loc(), "xxx", "xyz");
		try {
			airport = airportRepository.withScope(scopeName).withCollection(collectionName).save(airport);
			otherAirport = airportRepository.withScope(scopeName).withCollection(collectionName).save(otherAirport);
			assertEquals(1,
					airportRepository.withScope(scopeName).withCollection(collectionName).deleteByIata(airport.getIata()).size());
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			airportRepository.withScope(scopeName).withCollection(collectionName).deleteById(otherAirport.getId());
		}
	}

	@Test
	void stringDeleteWithRepositoryAnnotationTest() {
		Airport airport = new Airport(loc(), "vie", "abc");
		Airport otherAirport = new Airport(loc(), "xxx", "xyz");
		try {
			airport = airportRepositoryAnnotated.withScope(scopeName).save(airport);
			otherAirport = airportRepositoryAnnotated.withScope(scopeName).save(otherAirport);
			// don't specify a collection - should get collection from AirportRepositoryAnnotated
			assertEquals(1, airportRepositoryAnnotated.withScope(scopeName).deleteByIata(airport.getIata()).size());
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			// this will fail if the above didn't use collectionName2
			airportRepository.withScope(scopeName).withCollection(collectionName2).deleteById(otherAirport.getId());
		}
	}

	@Test
	void stringDeleteWithMethodAnnotationTest() {
		Airport airport = new Airport(loc(), "vie", "abc");
		Airport otherAirport = new Airport(loc(), "xxx", "xyz");
		try {
			Airport airportSaved = airportRepositoryAnnotated.withScope(scopeName).save(airport);
			Airport otherAirportSaved = airportRepositoryAnnotated.withScope(scopeName).save(otherAirport);
			// don't specify a collection - should get collection from deleteByIataAnnotated method
			assertThrows(IndexFailureException.class, () -> assertEquals(1,
					airportRepositoryAnnotated.withScope(scopeName).deleteByIataAnnotated(airport.getIata()).size()));
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			// this will fail if the above didn't use collectionName2
			airportRepository.withScope(scopeName).withCollection(collectionName2).deleteById(otherAirport.getId());
		}
	}

}
