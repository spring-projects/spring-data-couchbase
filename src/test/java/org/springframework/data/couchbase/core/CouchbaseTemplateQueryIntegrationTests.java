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

package org.springframework.data.couchbase.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.data.couchbase.core.query.N1QLExpression.i;

import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.domain.Address;
import org.springframework.data.couchbase.domain.Airport;
import org.springframework.data.couchbase.domain.Course;
import org.springframework.data.couchbase.domain.NaiveAuditorAware;
import org.springframework.data.couchbase.domain.Submission;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserJustLastName;
import org.springframework.data.couchbase.domain.UserSubmission;
import org.springframework.data.couchbase.domain.UserSubmissionProjected;
import org.springframework.data.couchbase.domain.time.AuditingDateTimeProvider;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;

import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * Query tests Theses tests rely on a cb server running
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Haris Alesevic
 * @author Mauro Monti
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
class CouchbaseTemplateQueryIntegrationTests extends JavaIntegrationTests {

	@BeforeEach
	@Override
	public void beforeEach() {
		super.beforeEach();
		// already setup by JavaIntegrationTests.beforeAll()
		// ApplicationContext ac = new AnnotationConfigApplicationContext(Config.class);
		// couchbaseTemplate = (CouchbaseTemplate) ac.getBean(COUCHBASE_TEMPLATE);
		// reactiveCouchbaseTemplate = (ReactiveCouchbaseTemplate) ac.getBean(REACTIVE_COUCHBASE_TEMPLATE);
		// ensure each test starts with clean state

		couchbaseTemplate.removeByQuery(User.class).all();
		couchbaseTemplate.findByQuery(User.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
	}

	@Test
	void findByQueryAll() {
		try {
			User user1 = new User(UUID.randomUUID().toString(), "user1", "user1");
			User user2 = new User(UUID.randomUUID().toString(), "user2", "user2");

			couchbaseTemplate.upsertById(User.class).all(Arrays.asList(user1, user2));

			final List<User> foundUsers = couchbaseTemplate.findByQuery(User.class)
					.withConsistency(QueryScanConsistency.REQUEST_PLUS).all();

			for (User u : foundUsers) {
				if (!(u.equals(user1) || u.equals(user2))) {
					// somebody didn't clean up after themselves.
					couchbaseTemplate.removeById().one(u.getId());
				}
			}
			assertEquals(2, foundUsers.size());
			TemporalAccessor auditTime = new AuditingDateTimeProvider().getNow().get();
			long auditMillis = Instant.from(auditTime).toEpochMilli();
			String auditUser = new NaiveAuditorAware().getCurrentAuditor().get();

			for (User u : foundUsers) {
				assertTrue(u.equals(user1) || u.equals(user2));
				assertEquals(auditUser, u.getCreator());
				assertEquals(auditMillis, u.getCreatedDate());
				assertEquals(auditUser, u.getLastModifiedBy());
				assertEquals(auditMillis, u.getLastModifiedDate());
			}
			couchbaseTemplate.findById(User.class).one(user1.getId());
			reactiveCouchbaseTemplate.findById(User.class).one(user1.getId()).block();
		} finally {
			couchbaseTemplate.removeByQuery(User.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}

		User usery = couchbaseTemplate.findById(User.class).one("user1");
		assertNull(usery, "user1 should have been deleted");
		User userz = reactiveCouchbaseTemplate.findById(User.class).one("user2").block();
		assertNull(userz, "user2 should have been deleted");

	}

	@Test
	void findByMatchingQuery() {
		User user1 = new User(UUID.randomUUID().toString(), "user1", "user1");
		User user2 = new User(UUID.randomUUID().toString(), "user2", "user2");
		User specialUser = new User(UUID.randomUUID().toString(), "special", "special");

		couchbaseTemplate.upsertById(User.class).all(Arrays.asList(user1, user2, specialUser));

		Query specialUsers = new Query(QueryCriteria.where(i("firstname")).like("special"));
		final List<User> foundUsers = couchbaseTemplate.findByQuery(User.class)
				.withConsistency(QueryScanConsistency.REQUEST_PLUS).matching(specialUsers).all();

		assertEquals(1, foundUsers.size());
	}

	@Test
	void findByMatchingQueryProjected() {

		couchbaseTemplate.removeByQuery(UserSubmission.class).all();

		UserSubmission user = new UserSubmission();
		user.setId(UUID.randomUUID().toString());
		user.setUsername("dave");
		user.setRoles(Arrays.asList("role1", "role2"));
		Address address = new Address();
		address.setStreet("1234 Olcott Street");
		user.setAddress(address);
		user.setSubmissions(
				Arrays.asList(new Submission(UUID.randomUUID().toString(), user.getId(), "tid", "status", 123)));
		user.setCourses(Arrays.asList(new Course(UUID.randomUUID().toString(), user.getId(), "581"),
				new Course(UUID.randomUUID().toString(), user.getId(), "777")));
		couchbaseTemplate.upsertById(UserSubmission.class).one(user);

		Query daveUsers = new Query(QueryCriteria.where("username").like("dave"));

		final List<UserSubmissionProjected> foundUserSubmissions = couchbaseTemplate.findByQuery(UserSubmission.class)
				.as(UserSubmissionProjected.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).matching(daveUsers).all();
		assertEquals(1, foundUserSubmissions.size());
		assertEquals(user.getUsername(), foundUserSubmissions.get(0).getUsername());
		assertEquals(user.getId(), foundUserSubmissions.get(0).getId());
		assertEquals(user.getCourses(), foundUserSubmissions.get(0).getCourses());
		assertEquals(user.getAddress(), foundUserSubmissions.get(0).getAddress());

		couchbaseTemplate.removeByQuery(UserSubmission.class).all();

		User user1 = new User(UUID.randomUUID().toString(), "user1", "user1");
		User user2 = new User(UUID.randomUUID().toString(), "user2", "user2");
		User specialUser = new User(UUID.randomUUID().toString(), "special", "special");

		couchbaseTemplate.upsertById(User.class).all(Arrays.asList(user1, user2, specialUser));

		Query specialUsers = new Query(QueryCriteria.where("firstname").like("special"));
		final List<UserJustLastName> foundUsers = couchbaseTemplate.findByQuery(User.class).as(UserJustLastName.class)
				.withConsistency(QueryScanConsistency.REQUEST_PLUS).matching(specialUsers).all();
		assertEquals(1, foundUsers.size());

		final List<UserJustLastName> foundUsersReactive = reactiveCouchbaseTemplate.findByQuery(User.class)
				.as(UserJustLastName.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).matching(specialUsers).all()
				.collectList().block();
		assertEquals(1, foundUsersReactive.size());

	}

	@Test
	void removeByQueryAll() {
		User user1 = new User(UUID.randomUUID().toString(), "user1", "user1");
		User user2 = new User(UUID.randomUUID().toString(), "user2", "user2");

		couchbaseTemplate.upsertById(User.class).all(Arrays.asList(user1, user2));

		assertTrue(couchbaseTemplate.existsById().one(user1.getId()));
		assertTrue(couchbaseTemplate.existsById().one(user2.getId()));

		couchbaseTemplate.removeByQuery(User.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();

		assertNull(couchbaseTemplate.findById(User.class).one(user1.getId()));
		assertNull(couchbaseTemplate.findById(User.class).one(user2.getId()));

	}

	@Test
	void removeByMatchingQuery() {
		User user1 = new User(UUID.randomUUID().toString(), "user1", "user1");
		User user2 = new User(UUID.randomUUID().toString(), "user2", "user2");
		User specialUser = new User(UUID.randomUUID().toString(), "special", "special");

		couchbaseTemplate.upsertById(User.class).all(Arrays.asList(user1, user2, specialUser));

		assertTrue(couchbaseTemplate.existsById().one(user1.getId()));
		assertTrue(couchbaseTemplate.existsById().one(user2.getId()));
		assertTrue(couchbaseTemplate.existsById().one(specialUser.getId()));

		Query nonSpecialUsers = new Query(QueryCriteria.where(i("firstname")).notLike("special"));

		couchbaseTemplate.removeByQuery(User.class).withConsistency(QueryScanConsistency.REQUEST_PLUS)
				.matching(nonSpecialUsers).all();

		assertNull(couchbaseTemplate.findById(User.class).one(user1.getId()));
		assertNull(couchbaseTemplate.findById(User.class).one(user2.getId()));
		assertNotNull(couchbaseTemplate.findById(User.class).one(specialUser.getId()));

	}

	@Test
	void distinct() {
		String[] iatas = { "JFK", "IAD", "SFO", "SJC", "SEA", "LAX", "PHX" };
		String[] icaos = { "ic0", "ic1", "ic0", "ic1", "ic0", "ic1", "ic0" };

		try {
			for (int i = 0; i < iatas.length; i++) {
				Airport airport = new Airport("airports::" + iatas[i], iatas[i] /*iata*/, icaos[i] /* icao */);
				couchbaseTemplate.insertById(Airport.class).one(airport);
			}

			// distinct and count(distinct(...)) calls. use as() and consistentWith to verify fluent api
			// as the fluent api for Distinct is tricky

			// distinct icao
			List<Airport> airports1 = couchbaseTemplate.findByQuery(Airport.class).distinct(new String[] { "icao" })
					.as(Airport.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
			assertEquals(2, airports1.size());

			// distinct all-fields-in-Airport.class
			List<Airport> airports2 = couchbaseTemplate.findByQuery(Airport.class).distinct(new String[] {}).as(Airport.class)
					.withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
			assertEquals(7, airports2.size());

			// count( distinct { iata, icao } )
			long count1 = couchbaseTemplate.findByQuery(Airport.class).distinct(new String[] { "iata", "icao" })
					.as(Airport.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).count();
			assertEquals(7, count1);

			// count( distinct (all fields in icaoClass)
			Class icaoClass = (new Object() {
				String iata;
				String icao;
			}).getClass();
			long count2 = couchbaseTemplate.findByQuery(Airport.class).distinct(new String[] {}).as(icaoClass)
					.withConsistency(QueryScanConsistency.REQUEST_PLUS).count();
			assertEquals(7, count2);

		} finally {
			couchbaseTemplate.removeById()
					.all(Arrays.stream(iatas).map((iata) -> "airports::" + iata).collect(Collectors.toSet()));
		}
	}

	@Test
	void distinctReactive() {
		String[] iatas = { "JFK", "IAD", "SFO", "SJC", "SEA", "LAX", "PHX" };
		String[] icaos = { "ic0", "ic1", "ic0", "ic1", "ic0", "ic1", "ic0" };

		try {
			for (int i = 0; i < iatas.length; i++) {
				Airport airport = new Airport("airports::" + iatas[i], iatas[i] /*iata*/, icaos[i] /* icao */);
				reactiveCouchbaseTemplate.insertById(Airport.class).one(airport).block();
			}

			// distinct and count(distinct(...)) calls. use as() and consistentWith to verify fluent api
			// as the fluent api for Distinct is tricky

			// distinct icao
			List<Airport> airports1 = reactiveCouchbaseTemplate.findByQuery(Airport.class).distinct(new String[] { "icao" })
					.as(Airport.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all().collectList().block();
			assertEquals(2, airports1.size());

			// distinct all-fields-in-Airport.class
			List<Airport> airports2 = reactiveCouchbaseTemplate.findByQuery(Airport.class).distinct(new String[] {})
					.as(Airport.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all().collectList().block();
			assertEquals(7, airports2.size());

			// count( distinct icao )
			// not currently possible to have multiple fields in COUNT(DISTINCT field1, field2, ... ) due to MB43475
			long count1 = reactiveCouchbaseTemplate.findByQuery(Airport.class).distinct(new String[] { "icao" })
					.as(Airport.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).count().block();
			assertEquals(2, count1);

			// count( distinct (all fields in icaoClass) // which only has one field
			// not currently possible to have multiple fields in COUNT(DISTINCT field1, field2, ... ) due to MB43475
			Class icaoClass = (new Object() {
				String icao;
			}).getClass();
			long count2 = (long) reactiveCouchbaseTemplate.findByQuery(Airport.class).distinct(new String[] {}).as(icaoClass)
					.withConsistency(QueryScanConsistency.REQUEST_PLUS).count().block();
			assertEquals(2, count2);

		} finally {
			reactiveCouchbaseTemplate.removeById()
					.all(Arrays.stream(iatas).map((iata) -> "airports::" + iata).collect(Collectors.toSet())).collectList()
					.block();
		}
	}

}
