/*
 * Copyright 2012-2023 the original author or authors
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

import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;
import static org.awaitility.Awaitility.with;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.*;

import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.java.query.QueryScanConsistency;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.couchbase.core.ReactiveFindByIdOperation.ReactiveFindById;
import org.springframework.data.couchbase.core.ReactiveRemoveByIdOperation.ReactiveRemoveById;
import org.springframework.data.couchbase.core.ReactiveReplaceByIdOperation.ReactiveReplaceById;
import org.springframework.data.couchbase.core.support.OneAndAllEntityReactive;
import org.springframework.data.couchbase.core.support.OneAndAllIdReactive;
import org.springframework.data.couchbase.core.support.WithDurability;
import org.springframework.data.couchbase.core.support.WithExpiry;
import org.springframework.data.couchbase.domain.*;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

/**
 * KV tests Theses tests rely on a cb server running.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Tigran Babloyan
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(Config.class)
class ReactiveCouchbaseTemplateKeyValueIntegrationTests extends JavaIntegrationTests {

	@Autowired public CouchbaseTemplate couchbaseTemplate;
	@Autowired public ReactiveCouchbaseTemplate reactiveCouchbaseTemplate;

	@BeforeEach
	@Override
	public void beforeEach() {
		super.beforeEach();
		List<RemoveResult> r1 = reactiveCouchbaseTemplate.removeByQuery(User.class).withConsistency(REQUEST_PLUS).all()
				.collectList().block();
		List<RemoveResult> r2 = reactiveCouchbaseTemplate.removeByQuery(UserAnnotated.class).withConsistency(REQUEST_PLUS)
				.all().collectList().block();
		List<RemoveResult> r3 = reactiveCouchbaseTemplate.removeByQuery(UserAnnotated2.class).withConsistency(REQUEST_PLUS)
				.all().collectList().block();
		List<UserAnnotated2> f3 = reactiveCouchbaseTemplate.findByQuery(UserAnnotated2.class).withConsistency(REQUEST_PLUS)
				.all().collectList().block();
	}

	@Test
	void findByIdWithExpiry() {
		try {
			User user1 = new User(UUID.randomUUID().toString(), "user1", "user1");
			User user2 = new User(UUID.randomUUID().toString(), "user2", "user2");

			Collection<User> upserts = (Collection<User>) reactiveCouchbaseTemplate.upsertById(User.class)
					.all(Arrays.asList(user1, user2)).collectList().block();

			User foundUser = reactiveCouchbaseTemplate.findById(User.class).withExpiry(Duration.ofSeconds(1))
					.one(user1.getId()).block();
			user1.setVersion(foundUser.getVersion());// version will have changed
			assertEquals(user1, foundUser);

			int tries = 0;
			Collection<User> foundUsers;
			do {
				sleepSecs(1);
				foundUsers = (Collection<User>) reactiveCouchbaseTemplate.findById(User.class)
						.all(Arrays.asList(user1.getId(), user2.getId())).collectList().block();
			} while (tries++ < 10 && foundUsers.size() != 1 && !user2.equals(foundUsers.iterator().next()));
			assertEquals(1, foundUsers.size(), "should have found exactly 1 user");
			assertEquals(user2, foundUsers.iterator().next());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(User.class).withConsistency(REQUEST_PLUS).all().collectList().block();
		}

	}

	@Test
	void findByIdWithLock() {
		try {
			User user = new User(UUID.randomUUID().toString(), "user1", "user1");

			reactiveCouchbaseTemplate.upsertById(User.class).one(user).block();

			User foundUser = reactiveCouchbaseTemplate.findById(User.class).withLock(Duration.ofSeconds(4))
					.one(user.getId()).block();
			user.setVersion(foundUser.getVersion());// version will have changed
			assertEquals(user, foundUser);

			TimeoutException exception = assertThrows(TimeoutException.class, () -> 
				reactiveCouchbaseTemplate.upsertById(User.class).one(user).block()
			);
			assertTrue(exception.retryReasons().contains(RetryReason.KV_LOCKED), "should have been locked");
		} finally {
			// cleanup
			with()
					.pollInterval(Duration.ofSeconds(1))
					.pollDelay(Duration.ofSeconds(3))
					.await()
					.atMost(Duration.ofSeconds(8))
					.ignoreExceptions()
					.until(() -> reactiveCouchbaseTemplate.removeByQuery(User.class).withConsistency(REQUEST_PLUS)
							.all().collectList().block().size() == 1);
		}

	}

	@Test
	void upsertAndFindById() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User modified = reactiveCouchbaseTemplate.upsertById(User.class).one(user).block();
		assertEquals(user, modified);
		// create a new object so that the object returned by replaceById() is a different object from the original user
		// don't need to copy the ModifiedDate/ModifiedTime as they are not read and are overwritten.
		User modifying = new User(user.getId(), user.getFirstname(), user.getLastname());
		modifying.setCreatedDate(user.getCreatedDate());
		modifying.setCreatedBy(user.getCreatedBy());
		modifying.setVersion(user.getVersion());
		modified = reactiveCouchbaseTemplate.replaceById(User.class).one(modifying).block();
		assertEquals(modifying, modified);
		if (user == modified) {
			throw new RuntimeException(" user == modified ");
		}
		assertNotEquals(user, modified);
		assertEquals(ReactiveNaiveAuditorAware.AUDITOR, modified.getCreatedBy());
		assertEquals(ReactiveNaiveAuditorAware.AUDITOR, modified.getLastModifiedBy());
		assertNotEquals(0, modified.getCreatedDate());
		assertNotEquals(0, modified.getLastModifiedDate());
		// The FixedDateTimeService of the AuditingDateTimeProvider will guarantee these are equal
		assertEquals(user.getLastModifiedDate(), modified.getLastModifiedDate());

		User badUser = new User(user.getId(), user.getFirstname(), user.getLastname());
		badUser.setVersion(12345678);
		assertThrows(OptimisticLockingFailureException.class,
				() -> reactiveCouchbaseTemplate.replaceById(User.class).one(badUser).block());

		User found = reactiveCouchbaseTemplate.findById(User.class).one(user.getId()).block();
		user.setVersion(found.getVersion());
		assertEquals(modified, found);

		reactiveCouchbaseTemplate.removeById().one(user.getId()).block();
	}

	@Test
	void withDurability()
			throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
		for (Class<?> clazz : new Class[] { User.class, UserAnnotatedDurability.class, UserAnnotatedPersistTo.class, UserAnnotatedReplicateTo.class }) {
			// insert, replace, upsert
			for (OneAndAllEntityReactive<User> operator : new OneAndAllEntityReactive[]{
					reactiveCouchbaseTemplate.insertById(clazz), reactiveCouchbaseTemplate.replaceById(clazz),
					reactiveCouchbaseTemplate.upsertById(clazz)}) {
				// create an entity of type clazz
				Constructor<?> cons = clazz.getConstructor(String.class, String.class, String.class);
				User user = (User) cons.newInstance("" + operator.getClass().getSimpleName() + "_" + clazz.getSimpleName(),
						"firstname", "lastname");

				if (clazz.equals(User.class)) { // User.java doesn't have an durability annotation
					operator = (OneAndAllEntityReactive<User>) ((WithDurability<User>) operator).withDurability(PersistTo.ACTIVE,
							ReplicateTo.NONE);
				} else if (clazz.equals(UserAnnotatedReplicateTo.class)){ // override the replica count from the annotation with no replica
					operator = (OneAndAllEntityReactive<User>) ((WithDurability<User>) operator).withDurability(PersistTo.NONE,
							ReplicateTo.NONE);
				}

				// if replace, we need to insert a document to replace
				if (operator instanceof ReactiveReplaceById) {
					reactiveCouchbaseTemplate.insertById(User.class).one(user).block();
				}
				// call to insert/replace/update
				User returned = operator.one(user).block();
				assertEquals(user, returned);
				User found = reactiveCouchbaseTemplate.findById(User.class).one(user.getId()).block();
				assertEquals(user, found);

				if (operator instanceof ReactiveReplaceByIdOperation.ReactiveReplaceById) {
					if (clazz.equals(UserAnnotatedReplicateTo.class)){ // override the replica count from the annotation with no replica
						reactiveCouchbaseTemplate.removeById(clazz).withDurability(PersistTo.NONE,
								ReplicateTo.NONE).one(user.getId()).block();
					} else {
						reactiveCouchbaseTemplate.removeById(clazz).one(user.getId()).block();	
					}
					User removed = reactiveCouchbaseTemplate.findById(user.getClass()).one(user.getId()).block();
					assertNull(removed, "found should have been null as document should be removed");
				}
			}
		}

	}

	@Test
	void withExpiryAndExpiryAnnotation()
			throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
		// ( withExpiry()<User>, expiry=1<UserAnnotated>, expiryExpression=${myExpiry}<UserAnnotated2> ) X ( insert,
		// replace, upsert )
		Set<User> users = new HashSet<>(); // set of all documents we will insert
		// Entity classes
		for (Class<?> clazz : new Class[] { User.class, UserAnnotated.class, UserAnnotated2.class, UserAnnotated3.class }) {
			// insert, replace, upsert
			for (Object operator : new Object[] { reactiveCouchbaseTemplate.insertById(clazz),
					reactiveCouchbaseTemplate.replaceById(clazz), reactiveCouchbaseTemplate.upsertById(clazz),
					reactiveCouchbaseTemplate.findById(clazz) }) {

				// create an entity of type clazz
				Constructor<?> cons = clazz.getConstructor(String.class, String.class, String.class);
				User user = (User) cons.newInstance("" + operator.getClass().getSimpleName() + "_" + clazz.getSimpleName(),
						"firstname", "lastname");

				if (clazz.equals(User.class)) { // User.java doesn't have an expiry annotation
					operator = ((WithExpiry<User>) operator).withExpiry(Duration.ofSeconds(1));
				} else if (clazz.equals(UserAnnotated3.class)) { // override the expiry from the annotation with no expiry
					operator = ((WithExpiry<User>) operator).withExpiry(Duration.ofSeconds(0));
				}

				// if replace, remove or find, we need to insert a document first
				if (operator instanceof ReactiveReplaceById || operator instanceof ReactiveRemoveById
						|| operator instanceof ReactiveFindById) {
					user = reactiveCouchbaseTemplate.insertById(User.class).one(user).block();
				}
				// call to insert/replace/update/find
				User returned = operator instanceof OneAndAllEntityReactive
						? ((OneAndAllEntityReactive<User>) operator).one(user).block()
						: ((OneAndAllIdReactive<User>) operator).one(user.getId()).block();
				if (operator instanceof OneAndAllIdReactive) { // the user.version won't be updated
					user.setVersion(returned.getVersion());
				}
				assertEquals(user, returned);
				users.add(user);
			}
		}
		// check that they are gone after a few seconds.
		int tries = 0;
		List<String> errorList = new LinkedList<>();
		do {
			sleepSecs(1);
			for (User user : users) {
				errorList = new LinkedList<>();
				User found = reactiveCouchbaseTemplate.findById(user.getClass()).one(user.getId()).block();
				if (user.getId().endsWith(UserAnnotated3.class.getSimpleName())) {
					if (found == null) {
						errorList.add("\nfound should be non null as it was set to have no expiry " + user.getId());
					}
				} else {
					if (found != null) {
						errorList.add("\nfound should have been null as document should be expired " + user.getId());
					}
				}
				if (found != null) {
					couchbaseTemplate.removeById(user.getClass()).one(user.getId());
				}
			}

		} while (tries++ < 10 && !errorList.isEmpty());

		if (!errorList.isEmpty()) {
			throw new RuntimeException(errorList.toString());
		}
	}

	@Test
	void findDocWhichDoesNotExist() {
		assertNull(reactiveCouchbaseTemplate.findById(User.class).one(UUID.randomUUID().toString()).block());
	}

	@Test
	void upsertAndReplaceById() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User modified = reactiveCouchbaseTemplate.upsertById(User.class).one(user).block();
		assertEquals(user, modified);

		User toReplace = new User(modified.getId(), "some other", "lastname");
		reactiveCouchbaseTemplate.replaceById(User.class).one(toReplace).block();

		User loaded = reactiveCouchbaseTemplate.findById(User.class).one(toReplace.getId()).block();
		assertEquals("some other", loaded.getFirstname());

		reactiveCouchbaseTemplate.removeById().one(toReplace.getId()).block();

	}

	@Test
	void upsertAndRemoveById() {
		{
			User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
			User modified = reactiveCouchbaseTemplate.upsertById(User.class).one(user).block();
			assertEquals(user, modified);

			RemoveResult removeResult = reactiveCouchbaseTemplate.removeById().one(user.getId()).block();
			assertEquals(user.getId(), removeResult.getId());
			assertTrue(removeResult.getCas() != 0);
			assertTrue(removeResult.getMutationToken().isPresent());

			assertNull(reactiveCouchbaseTemplate.findById(User.class).one(user.getId()).block());
		}
		{
			User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
			User modified = reactiveCouchbaseTemplate.upsertById(User.class).one(user).block();
			assertEquals(user, modified);

			// careful now - user and modified are the same object. The object has the new cas (@Version version)
			Long savedCas = modified.getVersion();
			modified.setVersion(123);
			assertThrows(OptimisticLockingFailureException.class, () -> reactiveCouchbaseTemplate.removeById()
					.withCas(reactiveCouchbaseTemplate.support().getCas(modified)).one(modified.getId()).block());
			modified.setVersion(savedCas);
			reactiveCouchbaseTemplate.removeById().withCas(reactiveCouchbaseTemplate.support().getCas(modified))
					.one(modified.getId()).block();
		}
	}

	@Test
	void mutateInByIdUpsert(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withUpsertPaths("roles").one(mutableUser).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertEquals(user.getRoles(), mutableUser.getRoles());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdUpsertWithCAS(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			mutableUser.setVersion(999);
			// if case is incorrect then exception is thrown
			assertThrows(OptimisticLockingFailureException.class, ()-> reactiveCouchbaseTemplate.mutateInById(MutableUser.class)
					.withCasProvided().withUpsertPaths("roles").one(mutableUser).block());

			// success on correct cas
			mutableUser.setVersion(user.getVersion());
			reactiveCouchbaseTemplate.mutateInById(MutableUser.class)
					.withCasProvided().withUpsertPaths("roles").one(mutableUser).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertEquals(mutableUser.getRoles(), user.getRoles());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdUpsertMultiple() {
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address address = new Address();
			address.setCity("city");
			address.setStreet("street");
			mutableUser.setAddress(address);
			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withUpsertPaths("roles", "address.street").one(mutableUser).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertEquals(user.getRoles(), mutableUser.getRoles());
			assertEquals(user.getAddress().getStreet(), mutableUser.getAddress().getStreet());
			assertNull(user.getAddress().getCity());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdUpsertMultipleWithDurability(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address address = new Address();
			address.setCity("city");
			address.setStreet("street");
			mutableUser.setAddress(address);
			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withDurability(DurabilityLevel.MAJORITY)
					.withUpsertPaths("roles", "address.street").one(mutableUser).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertEquals(user.getRoles(), mutableUser.getRoles());
			assertEquals(user.getAddress().getStreet(), mutableUser.getAddress().getStreet());
			assertNull(user.getAddress().getCity());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdUpsertMultipleWithExpiry(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address address = new Address();
			address.setCity("city");
			address.setStreet("street");
			mutableUser.setAddress(address);
			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withExpiry(Duration.ofSeconds(1))
					.withUpsertPaths("roles", "address.street").one(mutableUser).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertEquals(user.getRoles(), mutableUser.getRoles());
			assertEquals(user.getAddress().getStreet(), mutableUser.getAddress().getStreet());
			assertNull(user.getAddress().getCity());

			// user should be gone
			int tries = 0;
			MutableUser foundUser;
			do {
				sleepSecs(2);
				foundUser = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();
			} while (tries++ < 5 && foundUser != null);
			assertNull(foundUser);
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdReplace(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withReplacePaths("firstname").one(mutableUser).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertEquals(mutableUser.getFirstname(), user.getFirstname());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdReplaceWithCAS(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setVersion(999);

			// if case is incorrect then exception is thrown
			assertThrows(OptimisticLockingFailureException.class, ()-> reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withCasProvided()
					.withReplacePaths("firstname").one(mutableUser).block());

			// success on correct cas
			mutableUser.setVersion(user.getVersion());
			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withCasProvided().withReplacePaths("firstname").one(mutableUser).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertEquals(mutableUser.getFirstname(), user.getFirstname());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdReplaceMultiple() {
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			Address address = new Address();
			address.setCity("city");
			address.setStreet("street");
			user.setAddress(address);
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address mutableAddress = new Address();
			mutableAddress.setCity("othercity");
			mutableUser.setAddress(mutableAddress);
			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withReplacePaths("firstname", "address.city")
					.one(mutableUser).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertEquals(mutableUser.getFirstname(), user.getFirstname());
			assertEquals(user.getAddress().getCity(), mutableUser.getAddress().getCity());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdReplaceMultipleWithDurability(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			Address address = new Address();
			address.setCity("city");
			address.setStreet("street");
			user.setAddress(address);
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address mutableAddress = new Address();
			mutableAddress.setCity("othercity");
			mutableUser.setAddress(mutableAddress);
			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withDurability(DurabilityLevel.MAJORITY)
					.withReplacePaths("firstname", "address.city").one(mutableUser).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertEquals(mutableUser.getFirstname(), user.getFirstname());
			assertEquals(user.getAddress().getCity(), mutableUser.getAddress().getCity());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdReplaceMultipleWithExpiry(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			Address address = new Address();
			address.setCity("city");
			user.setAddress(address);
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address mutableAddress = new Address();
			mutableAddress.setCity("othercity");
			mutableAddress.setStreet("street");
			mutableUser.setAddress(mutableAddress);
			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withExpiry(Duration.ofSeconds(1))
					.withReplacePaths("firstname", "address.city").one(mutableUser).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertEquals(mutableUser.getFirstname(), user.getFirstname());
			assertEquals(user.getAddress().getCity(), mutableUser.getAddress().getCity());
			assertNull(user.getAddress().getStreet());

			// user should be gone
			int tries = 0;
			MutableUser foundUser;
			do {
				sleepSecs(2);
				foundUser = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();
			} while (tries++ < 5 && foundUser != null);
			assertNull(foundUser);
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdReplaceMissingPath(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address mutableAddress = new Address();
			mutableAddress.setCity("othercity");
			mutableAddress.setStreet("street");
			mutableUser.setAddress(mutableAddress);

			assertThrows(InvalidDataAccessResourceUsageException.class, 
					()-> reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withReplacePaths("roles", "address.city")
							.one(mutableUser).block());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdInsert(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withInsertPaths("roles").one(mutableUser).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertEquals(mutableUser.getRoles(), user.getRoles());
			assertNotEquals(mutableUser.getFirstname(), user.getFirstname());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdInsertWithCAS(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			mutableUser.setVersion(999);

			// if case is incorrect then exception is thrown
			assertThrows(OptimisticLockingFailureException.class, ()-> reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withCasProvided()
					.withInsertPaths("roles").one(mutableUser).block());

			// success on correct cas
			mutableUser.setVersion(user.getVersion());
			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withCasProvided().withInsertPaths("roles").one(mutableUser).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertEquals(mutableUser.getRoles(), user.getRoles());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdInsertMultiple() {
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address mutableAddress = new Address();
			mutableAddress.setCity("othercity");
			mutableAddress.setStreet("street");
			mutableUser.setAddress(mutableAddress);
			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withInsertPaths("roles", "address.city").one(mutableUser).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertEquals(mutableUser.getRoles(), user.getRoles());
			assertEquals(user.getAddress().getCity(), mutableUser.getAddress().getCity());
			assertNull(user.getAddress().getStreet());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdInsertMultipleWithDurability(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address mutableAddress = new Address();
			mutableAddress.setCity("othercity");
			mutableAddress.setStreet("street");
			mutableUser.setAddress(mutableAddress);
			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withDurability(DurabilityLevel.MAJORITY)
					.withInsertPaths("address.city").one(mutableUser).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertEquals(user.getAddress().getCity(), mutableUser.getAddress().getCity());
			assertNull(user.getAddress().getStreet());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdInsertMultipleWithExpiry(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address mutableAddress = new Address();
			mutableAddress.setCity("othercity");
			mutableAddress.setStreet("street");
			mutableUser.setAddress(mutableAddress);

			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withExpiry(Duration.ofSeconds(1))
					.withInsertPaths("address.city").one(mutableUser).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertEquals(user.getAddress().getCity(), mutableUser.getAddress().getCity());
			assertNull(user.getAddress().getStreet());

			// user should be gone
			int tries = 0;
			MutableUser foundUser;
			do {
				sleepSecs(2);
				foundUser = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();
			} while (tries++ < 5 && foundUser != null);
			assertNull(foundUser);
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdInsertMissingPath(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");

			assertThrows(InvalidDataAccessResourceUsageException.class, ()-> reactiveCouchbaseTemplate.mutateInById(MutableUser.class)
					.withInsertPaths("firstname").one(mutableUser).block());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdRemove(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user.setRoles(Collections.singletonList("ADMIN"));
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			assertNotNull(user.getRoles());

			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withRemovePaths("roles").one(user).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertNull(user.getRoles());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdRemoveWithCAS(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user.setRoles(Collections.singletonList("ADMIN"));
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();
			String userId = user.getId();

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "otherlast");
			mutableUser.setVersion(999);
			// if case is incorrect then exception is thrown
			assertThrows(OptimisticLockingFailureException.class, ()-> reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withCasProvided()
					.withRemovePaths("roles").one(mutableUser).block());

			// success on correct cas
			user.setVersion(user.getVersion());
			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withCasProvided().withRemovePaths("roles").one(user).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(userId).block();

			assertNull(user.getRoles());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdRemoveMultiple() {
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user.setRoles(Collections.singletonList("ADMIN"));
			Address mutableAddress = new Address();
			mutableAddress.setCity("othercity");
			mutableAddress.setStreet("street");
			user.setAddress(mutableAddress);
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withRemovePaths("roles", "address.city").one(user).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertNull(user.getRoles());
			assertNull(user.getAddress().getCity());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdRemoveMultipleWithDurability(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withDurability(DurabilityLevel.MAJORITY)
					.withRemovePaths("lastname").one(user).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertNull(user.getLastname());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdRemoveMultipleWithExpiry(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			reactiveCouchbaseTemplate.mutateInById(MutableUser.class).withExpiry(Duration.ofSeconds(1))
					.withRemovePaths("lastname").one(user).block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();
			assertNull(user.getLastname());

			// user should be gone
			int tries = 0;
			MutableUser foundUser;
			do {
				sleepSecs(2);
				foundUser = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();
			} while (tries++ < 5 && foundUser != null);
			assertNull(foundUser);
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdRemoveMissingPath(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");

			assertThrows(InvalidDataAccessResourceUsageException.class, ()-> reactiveCouchbaseTemplate.mutateInById(MutableUser.class)
					.withRemovePaths("roles").one(mutableUser).block());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdChained(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			Address address = new Address();
			address.setCity("city");
			address.setStreet("street");
			user.setAddress(address);
			user = reactiveCouchbaseTemplate.insertById(MutableUser.class).one(user).block();

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address mAddress = new Address();
			mAddress.setCity("othercity");
			mAddress.setStreet("otherstreet");
			mutableUser.setAddress(mAddress);
			mutableUser.setSubuser(new MutableUser("subuser", "subfirstname", "sublastname"));

			reactiveCouchbaseTemplate.mutateInById(MutableUser.class)
					.withInsertPaths("roles", "subuser.firstname")
					.withRemovePaths("address.city")
					.withUpsertPaths("firstname")
					.withReplacePaths("address.street")
					.one(mutableUser)
					.block();

			user = reactiveCouchbaseTemplate.findById(MutableUser.class).one(user.getId()).block();

			assertNull(user.getAddress().getCity());
			assertEquals(mAddress.getStreet(),  user.getAddress().getStreet());
			assertEquals(mutableUser.getRoles(),  user.getRoles());
			assertEquals(mutableUser.getFirstname(),  user.getFirstname());
			assertEquals(mutableUser.getSubuser().getFirstname(),  user.getSubuser().getFirstname());
			assertNull(user.getSubuser().getLastname());
		} finally {
			reactiveCouchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void insertById() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User inserted = reactiveCouchbaseTemplate.insertById(User.class).one(user).block();
		assertEquals(user, inserted);
		assertThrows(DuplicateKeyException.class, () -> reactiveCouchbaseTemplate.insertById(User.class).one(user).block());
	}

	@Test
	void insertByIdwithDurability() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User inserted = reactiveCouchbaseTemplate.insertById(User.class).withDurability(PersistTo.ACTIVE, ReplicateTo.NONE)
				.one(user).block();
		assertEquals(user, inserted);
		assertThrows(DuplicateKeyException.class, () -> reactiveCouchbaseTemplate.insertById(User.class).one(user).block());
	}

	@Test
	void insertByIdWithAnnotatedDurability() {
		UserAnnotatedPersistTo user = new UserAnnotatedPersistTo(UUID.randomUUID().toString(), "firstname", "lastname");
		UserAnnotatedPersistTo inserted = reactiveCouchbaseTemplate.insertById(UserAnnotatedPersistTo.class)
				.one(user).block();
		assertEquals(user, inserted);
		assertThrows(DuplicateKeyException.class, () -> reactiveCouchbaseTemplate.insertById(User.class).one(user).block());
	}

	@Test
	void insertByIdWithAnnotatedDurability2() {
		UserAnnotatedDurability user = new UserAnnotatedDurability(UUID.randomUUID().toString(), "firstname", "lastname");
		UserAnnotatedDurability inserted = reactiveCouchbaseTemplate.insertById(UserAnnotatedDurability.class)
				.one(user).block();
		assertEquals(user, inserted);
		assertThrows(DuplicateKeyException.class, () -> reactiveCouchbaseTemplate.insertById(User.class).one(user).block());
	}

	@Test
	void existsById() {
		String id = UUID.randomUUID().toString();
		assertFalse(reactiveCouchbaseTemplate.existsById().one(id).block());

		User user = new User(id, "firstname", "lastname");
		User inserted = reactiveCouchbaseTemplate.insertById(User.class).one(user).block();
		assertEquals(user, inserted);

		assertTrue(reactiveCouchbaseTemplate.existsById().one(id).block());

	}

	@Test
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void saveAndFindImmutableById() {
		PersonValue personValue = new PersonValue(UUID.randomUUID().toString(), 123, "329", "l");
		PersonValue inserted;
		PersonValue upserted;
		PersonValue replaced;

		inserted = reactiveCouchbaseTemplate.insertById(PersonValue.class).one(personValue).block();
		assertNotEquals(0, inserted.getVersion());
		PersonValue foundInserted = reactiveCouchbaseTemplate.findById(PersonValue.class).one(inserted.getId()).block();
		assertNotNull(foundInserted, "inserted personValue not found");
		assertEquals(inserted, foundInserted);

		// upsert will insert
		reactiveCouchbaseTemplate.removeById().one(inserted.getId());
		upserted = reactiveCouchbaseTemplate.upsertById(PersonValue.class).one(inserted).block();
		assertNotEquals(0, upserted.getVersion());
		PersonValue foundUpserted = reactiveCouchbaseTemplate.findById(PersonValue.class).one(upserted.getId()).block();
		assertNotNull(foundUpserted, "upserted personValue not found");
		assertEquals(upserted, foundUpserted);

		// upsert will replace
		upserted = reactiveCouchbaseTemplate.upsertById(PersonValue.class).one(inserted).block();
		assertNotEquals(0, upserted.getVersion());
		PersonValue foundUpserted2 = reactiveCouchbaseTemplate.findById(PersonValue.class).one(upserted.getId()).block();
		assertNotNull(foundUpserted2, "upserted personValue not found");
		assertEquals(upserted, foundUpserted2);

		replaced = reactiveCouchbaseTemplate.replaceById(PersonValue.class).one(upserted).block();
		assertNotEquals(0, replaced.getVersion());
		PersonValue foundReplaced = reactiveCouchbaseTemplate.findById(PersonValue.class).one(replaced.getId()).block();
		assertNotNull(foundReplaced, "replaced personValue not found");
		assertEquals(replaced, foundReplaced);
		couchbaseTemplate.removeById(PersonValue.class).one(replaced.getId());
	}

	private void sleepSecs(int i) {
		try {
			Thread.sleep(i * 1000);
		} catch (InterruptedException ie) {}
	}

}
