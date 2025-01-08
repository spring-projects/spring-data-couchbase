/*
 * Copyright 2012-2025 the original author or authors
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.data.couchbase.core.query.N1QLExpression.i;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.couchbase.core.ExecutableFindByIdOperation.ExecutableFindById;
import org.springframework.data.couchbase.core.ExecutableRemoveByIdOperation.ExecutableRemoveById;
import org.springframework.data.couchbase.core.ExecutableReplaceByIdOperation.ExecutableReplaceById;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.core.support.OneAndAllEntity;
import org.springframework.data.couchbase.core.support.OneAndAllId;
import org.springframework.data.couchbase.core.support.WithDurability;
import org.springframework.data.couchbase.core.support.WithExpiry;
import org.springframework.data.couchbase.domain.Address;
import org.springframework.data.couchbase.domain.Config;
import org.springframework.data.couchbase.domain.MutableUser;
import org.springframework.data.couchbase.domain.NaiveAuditorAware;
import org.springframework.data.couchbase.domain.PersonValue;
import org.springframework.data.couchbase.domain.Submission;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserAnnotated;
import org.springframework.data.couchbase.domain.UserAnnotated2;
import org.springframework.data.couchbase.domain.UserAnnotated3;
import org.springframework.data.couchbase.domain.UserAnnotatedDurability;
import org.springframework.data.couchbase.domain.UserAnnotatedDurabilityExpression;
import org.springframework.data.couchbase.domain.UserAnnotatedPersistTo;
import org.springframework.data.couchbase.domain.UserAnnotatedReplicateTo;
import org.springframework.data.couchbase.domain.UserAnnotatedTouchOnRead;
import org.springframework.data.couchbase.domain.UserNoAlias;
import org.springframework.data.couchbase.domain.UserSubmission;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * KV test - these tests rely on a cb server running.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Tigran Babloyan
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(Config.class)
@DirtiesContext
@TestPropertySource(properties = { "valid.document.durability = MAJORITY" })
class CouchbaseTemplateKeyValueIntegrationTests extends JavaIntegrationTests {

	@Autowired public CouchbaseTemplate couchbaseTemplate;
	@Autowired public ReactiveCouchbaseTemplate reactiveCouchbaseTemplate;

	@BeforeEach
	@Override
	public void beforeEach() {
		super.beforeEach();
		couchbaseTemplate.removeByQuery(User.class).all();
		couchbaseTemplate.removeByQuery(UserAnnotated.class).all();
		couchbaseTemplate.removeByQuery(UserAnnotated2.class).all();
		couchbaseTemplate.removeByQuery(UserAnnotated3.class).all();
		couchbaseTemplate.removeByQuery(User.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
	}

	@Test
	void findByIdWithLock() {
		try {
			User user = new User("1", "user1", "user1");

			couchbaseTemplate.upsertById(User.class).one(user);

			User foundUser = couchbaseTemplate.findById(User.class).withLock(Duration.ofSeconds(5)).one(user.getId());
			user.setVersion(foundUser.getVersion());// version will have changed
			assertEquals(user, foundUser);
			
			TimeoutException exception = assertThrows(TimeoutException.class, () -> 
				couchbaseTemplate.upsertById(User.class).one(user)
			);
			assertTrue(exception.retryReasons().contains(RetryReason.KV_LOCKED), "should have been locked");
		} finally {
			for(int i=0; i< 10; i++) {
				sleepSecs(2);
				try {
					couchbaseTemplate.removeByQuery(User.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
					break;
				} catch (Exception e) {
					e.printStackTrace(); // gives IndexFailureException if the lock is still active
				}
			}
		}

	}

    @Test
    void findByIdNoAlias() {
        String firstname = UUID.randomUUID().toString();
        try {
            UserNoAlias user = new UserNoAlias("1", firstname, "user1");
            couchbaseTemplate.upsertById(UserNoAlias.class).one(user);
            UserNoAlias foundUser = couchbaseTemplate.findById(UserNoAlias.class).one(user.getId());
            user.setVersion(foundUser.getVersion());// version will have changed
            assertEquals(user, foundUser);
            Query query = new Query(QueryCriteria.where(i("firstname")).eq(firstname));
            List<UserNoAlias> queriedUsers = couchbaseTemplate.findByQuery(UserNoAlias.class)
                    .withConsistency(QueryScanConsistency.REQUEST_PLUS).matching(query).all();
            assertEquals(1, queriedUsers.size(), "should have found exactly one");
        } finally {
            Query query = new Query(QueryCriteria.where(i("firstname")).eq(firstname));
            List<RemoveResult> removeResult = couchbaseTemplate.removeByQuery(UserNoAlias.class)
                    .withConsistency(QueryScanConsistency.REQUEST_PLUS).matching(query).all();
            assertEquals(1, removeResult.size(), "should have removed exactly one");
        }
    }

	@Test
	void findByIdWithExpiry() {
		try {
			User user1 = new User(UUID.randomUUID().toString(), "user1", "user1");
			User user2 = new User(UUID.randomUUID().toString(), "user2", "user2");

			Collection<User> upserts = (Collection<User>) couchbaseTemplate.upsertById(User.class)
					.all(Arrays.asList(user1, user2));

			User foundUser = couchbaseTemplate.findById(User.class).withExpiry(Duration.ofSeconds(1)).one(user1.getId());
			user1.setVersion(foundUser.getVersion());// version will have changed
			assertEquals(user1, foundUser);
			int tries = 0;
			Collection<User> foundUsers;
			do {
				sleepSecs(1);
				foundUsers = (Collection<User>) couchbaseTemplate.findById(User.class)
						.all(Arrays.asList(user1.getId(), user2.getId()));
			} while (tries++ < 10 && foundUsers.size() != 1 && !user2.equals(foundUsers.iterator().next()));
			assertEquals(1, foundUsers.size(), "should have found exactly 1 user");
			assertEquals(user2, foundUsers.iterator().next());
		} finally {
			//couchbaseTemplate.removeByQuery(User.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}

	}

	@Test
	void findByIdWithExpiryAnnotation() {
		try {
			UserAnnotated user1 = new UserAnnotated(UUID.randomUUID().toString(), "user1", "user1");
			UserAnnotated user2 = new UserAnnotated(UUID.randomUUID().toString(), "user2", "user2");

			Collection<UserAnnotated> upserts = (Collection<UserAnnotated>) couchbaseTemplate.upsertById(UserAnnotated.class)
				.all(Arrays.asList(user1, user2));

			// explicitly set expiry to 10 seconds
			UserAnnotated foundUser1 = couchbaseTemplate.findById(UserAnnotated.class).withExpiry(Duration.ofSeconds(10)).one(user1.getId());
			user1.setVersion(foundUser1.getVersion());// version will have changed
			assertEquals(user1, foundUser1);
			UserAnnotated foundUser2 = couchbaseTemplate.findById(UserAnnotated.class).withExpiry(Duration.ofSeconds(10)).one(user2.getId());
			user2.setVersion(foundUser2.getVersion());// version will have changed
			assertEquals(user2, foundUser2);

			// now set user1 expiration back to 1 second with getAndTouch using the @Document(expiry=1) annotation
			// This will have no effect as UserAnnotated does not have touchOnGet
			foundUser1 = couchbaseTemplate.findById(UserAnnotated.class).one(user1.getId());
			user1.setVersion(foundUser1.getVersion());// version will have changed
			assertEquals(user1, foundUser1);

			// user1 should be gone, user2 should still be there
			int tries = 0;
			Collection<UserAnnotated> foundUsers;
			do {
				sleepSecs(3);
				foundUsers = (Collection<UserAnnotated>) couchbaseTemplate.findById(UserAnnotated.class)
					.all(Arrays.asList(user1.getId(), user2.getId()));
			} while (tries++ < 7 && foundUsers.size() != 2 && !user2.equals(foundUsers.iterator().next()));
			assertEquals(2, foundUsers.size(), "should have found exactly 2 users");
		} finally {
			couchbaseTemplate.removeByQuery(UserAnnotated.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void findByIdWithExpiryAnnotationTouchOnRead() {
		try {
			UserAnnotatedTouchOnRead user1 = new UserAnnotatedTouchOnRead(UUID.randomUUID().toString(), "user1", "user1");
			UserAnnotatedTouchOnRead user2 = new UserAnnotatedTouchOnRead(UUID.randomUUID().toString(), "user2", "user2");

			Collection<UserAnnotatedTouchOnRead> upserts = (Collection<UserAnnotatedTouchOnRead>) couchbaseTemplate.upsertById(UserAnnotatedTouchOnRead.class)
				.all(Arrays.asList(user1, user2));

			// explicitly set expiry to 10 seconds
			UserAnnotatedTouchOnRead foundUser1 = couchbaseTemplate.findById(UserAnnotatedTouchOnRead.class).withExpiry(Duration.ofSeconds(10)).one(user1.getId());
			user1.setVersion(foundUser1.getVersion());// version will have changed
			assertEquals(user1, foundUser1);
			UserAnnotatedTouchOnRead foundUser2 = couchbaseTemplate.findById(UserAnnotatedTouchOnRead.class).withExpiry(Duration.ofSeconds(10)).one(user2.getId());
			user2.setVersion(foundUser2.getVersion());// version will have changed
			assertEquals(user2, foundUser2);

			// now set user1 expiration back to 1 second with getAndTouch using the @Document(expiry=1) annotation
			foundUser1 = couchbaseTemplate.findById(UserAnnotatedTouchOnRead.class).one(user1.getId());
			user1.setVersion(foundUser1.getVersion());// version will have changed
			assertEquals(user1, foundUser1);

			// user1 should be gone, user2 should still be there
			int tries = 0;
			Collection<UserAnnotatedTouchOnRead> foundUsers;
			do {
				sleepSecs(3);
				foundUsers = (Collection<UserAnnotatedTouchOnRead>) couchbaseTemplate.findById(UserAnnotatedTouchOnRead.class)
					.all(Arrays.asList(user1.getId(), user2.getId()));
			} while (tries++ < 7 && foundUsers.size() != 1 && !user2.equals(foundUsers.iterator().next()));
			assertEquals(1, foundUsers.size(), "should have found exactly 1 user1");
			assertEquals(user2.getId(), foundUsers.iterator().next().getId());
		} finally {
			couchbaseTemplate.removeByQuery(UserAnnotatedTouchOnRead.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}
	@Test
	void upsertAndFindById() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User modified = couchbaseTemplate.upsertById(User.class).one(user);
		assertEquals(user, modified);
		// create a new object so that the object returned by replaceById() is a different object from the original user
		// don't need to copy the ModifiedDate/ModifiedTime as they are not read and are overwritten.
		User modifying = new User(user.getId(), user.getFirstname(), user.getLastname());
		modifying.setCreatedDate(user.getCreatedDate());
		modifying.setCreatedBy(user.getCreatedBy());
		modifying.setVersion(user.getVersion());
		modified = couchbaseTemplate.replaceById(User.class).one(modifying);
		assertEquals(modifying, modified);
		if (user == modified) {
			throw new RuntimeException(" user == modified ");
		}
		assertNotEquals(user, modified);
		assertEquals(NaiveAuditorAware.AUDITOR, modified.getCreatedBy());
		assertEquals(NaiveAuditorAware.AUDITOR, modified.getLastModifiedBy());
		assertNotEquals(0, modified.getCreatedDate());
		assertNotEquals(0, modified.getLastModifiedDate());
		// The FixedDateTimeService of the AuditingDateTimeProvider will guarantee these are equal
		assertEquals(user.getLastModifiedDate(), modified.getLastModifiedDate());

		User badUser = new User(user.getId(), user.getFirstname(), user.getLastname());
		badUser.setVersion(12345678);
		assertThrows(OptimisticLockingFailureException.class, () -> couchbaseTemplate.replaceById(User.class).one(badUser));

		User found = couchbaseTemplate.findById(User.class).one(user.getId());
		assertEquals(modified, found);

		couchbaseTemplate.removeById().one(user.getId());
	}

	@Test
	void findProjected() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		couchbaseTemplate.insertById(User.class).one(user);
		User found = couchbaseTemplate.findById(User.class).project(new String[] { "firstname" }).one(user.getId());
		System.err.println(found);
		couchbaseTemplate.removeById(User.class).one(user.getId());
	}

	@Test
	void findProjecting() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		couchbaseTemplate.insertById(User.class).one(user);
		List<User> found = couchbaseTemplate.findByQuery(User.class).project(new String[] { "firstname" })
				.withOptions(QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS)).all();
		assertEquals(1, found.size());
		assertNotEquals(user, found.get(0), "should have found this document");
		assertEquals(user.getFirstname(), found.get(0).getFirstname(), "firstname should match");
		assertNull(found.get(0).getLastname(), "lastname should be null");
		couchbaseTemplate.removeById(User.class).one(user.getId());
	}

	@Test
	void findProjectingPath() {
		UserSubmission user = new UserSubmission();
		user.setId(UUID.randomUUID().toString());
		user.setUsername("dave");
		user.setRoles(Arrays.asList("role1", "role2"));
		Address address = new Address();
		address.setStreet("1234 Olcott Street");
		address.setCity("Santa Clara");
		user.setAddress(address);
		user.setSubmissions(
				Arrays.asList(new Submission(UUID.randomUUID().toString(), user.getId(), "tid", "status", 123)));
		couchbaseTemplate.upsertById(UserSubmission.class).one(user);
		assertThrows(CouchbaseException.class, () -> couchbaseTemplate.findByQuery(UserSubmission.class)
				.project(new String[] { "address.street" }).withConsistency(QueryScanConsistency.REQUEST_PLUS).all());

		List<UserSubmission> found = couchbaseTemplate.findByQuery(UserSubmission.class).project(new String[] { "address" })
				.withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		assertEquals(found.size(), 1);
		assertEquals(found.get(0).getAddress(), address);
		assertNull(found.get(0).getUsername(), "username should have been null");
		couchbaseTemplate.removeById(User.class).one(user.getId());
	}

	@Test
	void withDurability()
			throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
		for (Class<?> clazz : new Class[] { User.class, UserAnnotatedDurability.class, UserAnnotatedDurabilityExpression.class, 
				UserAnnotatedPersistTo.class, UserAnnotatedReplicateTo.class }) {
			// insert, replace, upsert
			for (OneAndAllEntity<User> operator : new OneAndAllEntity[]{couchbaseTemplate.insertById(clazz),
					couchbaseTemplate.replaceById(clazz), couchbaseTemplate.upsertById(clazz)}) {
				// create an entity of type clazz
				Constructor<?> cons = clazz.getConstructor(String.class, String.class, String.class);
				User user = (User) cons.newInstance("" + operator.getClass().getSimpleName() + "_" + clazz.getSimpleName(),
						"firstname", "lastname");

				if (clazz.equals(User.class)) { // User.java doesn't have an durability annotation
					operator = (OneAndAllEntity<User>) ((WithDurability<User>) operator).withDurability(PersistTo.ACTIVE,
							ReplicateTo.NONE);
				} else if (clazz.equals(UserAnnotatedReplicateTo.class)){ // override the replica count from the annotation with no replica
					operator = (OneAndAllEntity<User>) ((WithDurability<User>) operator).withDurability(PersistTo.NONE,
							ReplicateTo.NONE);
				}

				// if replace, we need to insert a document to replace
				if (operator instanceof ExecutableReplaceById) {
					couchbaseTemplate.insertById(User.class).one(user);
				}
				// call to insert/replace/update
				User returned = null;

				// occasionally gives "reactor.core.Exceptions$OverflowException: Could not emit value due to lack of requests"
				for (int i = 1; i != 5; i++) {
					try {
						returned = (User) operator.one(user);
						break;
					} catch (Exception ofe) {
						System.out.println("" + i + " caught: " + ofe);
						couchbaseTemplate.removeByQuery(User.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
						if (i == 4) {
							throw ofe;
						}
						sleepSecs(1);
					}
				}
				assertEquals(user, returned);
				User found = couchbaseTemplate.findById(User.class).one(user.getId());
				assertEquals(user, found);

				if (operator instanceof ExecutableReplaceById) {
					if (clazz.equals(UserAnnotatedReplicateTo.class)){ // override the replica count from the annotation with no replica
						couchbaseTemplate.removeById(clazz).withDurability(PersistTo.ACTIVE,
								ReplicateTo.NONE).one(user.getId());
					} else {
						couchbaseTemplate.removeById(clazz).one(user.getId());
					}
					User removed = (User) couchbaseTemplate.findById(user.getClass()).one(user.getId());
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
			for (Object operator : new Object[] { couchbaseTemplate.insertById(clazz), couchbaseTemplate.replaceById(clazz),
					couchbaseTemplate.upsertById(clazz), couchbaseTemplate.findById(clazz) }) {

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
				if (operator instanceof ExecutableReplaceById || operator instanceof ExecutableRemoveById
						|| operator instanceof ExecutableFindById) {
					user = couchbaseTemplate.insertById(User.class).one(user);
				}

				// call to insert/replace/update/find
				User returned = operator instanceof OneAndAllEntity ? ((OneAndAllEntity<User>) operator).one(user)
						: ((OneAndAllId<User>) operator).one(user.getId());
				if (operator instanceof OneAndAllId) { // the user.version won't be updated
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
				User found = couchbaseTemplate.findById(user.getClass()).one(user.getId());
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
		assertNull(couchbaseTemplate.findById(User.class).one(UUID.randomUUID().toString()));
	}

	@Test
	void upsertAndReplaceById() {
		User user = new User(UUID.randomUUID().toString(), "firstname_upsertAndReplaceById", "lastname");
		User modified = couchbaseTemplate.upsertById(User.class).one(user);
		assertEquals(user, modified);

		User toReplace = new User(modified.getId(), "some other", "lastname");
		couchbaseTemplate.replaceById(User.class).one(toReplace);

		User loaded = couchbaseTemplate.findById(User.class).one(toReplace.getId());
		assertEquals("some other", loaded.getFirstname());

		couchbaseTemplate.removeById().one(toReplace.getId());

	}

	@Test
	void upsertAndRemoveById() {
		{
			User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
			User modified = couchbaseTemplate.upsertById(User.class).one(user);
			assertEquals(user, modified);

			RemoveResult removeResult = couchbaseTemplate.removeById().one(user.getId());
			assertEquals(user.getId(), removeResult.getId());
			assertTrue(removeResult.getCas() != 0);
			assertTrue(removeResult.getMutationToken().isPresent());
			assertNull(couchbaseTemplate.findById(User.class).one(user.getId()));
		}
		{
			User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
			User modified = couchbaseTemplate.upsertById(User.class).one(user);
			assertEquals(user, modified);

			// careful now - user and modified are the same object. The object has the new cas (@Version version)
			Long savedCas = modified.getVersion();
			modified.setVersion(123);
			assertThrows(OptimisticLockingFailureException.class, () -> couchbaseTemplate.removeById()
					.withCas(reactiveCouchbaseTemplate.support().getCas(modified)).one(modified.getId()));
			modified.setVersion(savedCas);
			couchbaseTemplate.removeById().withCas(reactiveCouchbaseTemplate.support().getCas(modified))
					.one(modified.getId());
		}
	}
	
	@Test
	void mutateInByIdUpsert(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			couchbaseTemplate.mutateInById(MutableUser.class).withUpsertPaths("roles").one(mutableUser);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertEquals(user.getRoles(), mutableUser.getRoles());
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdUpsertWithCAS(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			mutableUser.setVersion(999);
			// if case is incorrect then exception is thrown
			assertThrows(OptimisticLockingFailureException.class, ()-> couchbaseTemplate.mutateInById(MutableUser.class)
					.withCasProvided().withUpsertPaths("roles").one(mutableUser));

			// success on correct cas
			mutableUser.setVersion(user.getVersion());
			couchbaseTemplate.mutateInById(MutableUser.class)
					.withCasProvided().withUpsertPaths("roles").one(mutableUser);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertEquals(mutableUser.getRoles(), user.getRoles());
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdUpsertMultiple() {
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address address = new Address();
			address.setCity("city");
			address.setStreet("street");
			mutableUser.setAddress(address);
			couchbaseTemplate.mutateInById(MutableUser.class).withUpsertPaths("roles", "address.street").one(mutableUser);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertEquals(user.getRoles(), mutableUser.getRoles());
			assertEquals(user.getAddress().getStreet(), mutableUser.getAddress().getStreet());
			assertNull(user.getAddress().getCity());
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdUpsertMultipleWithDurability(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address address = new Address();
			address.setCity("city");
			address.setStreet("street");
			mutableUser.setAddress(address);
			couchbaseTemplate.mutateInById(MutableUser.class).withDurability(DurabilityLevel.MAJORITY).withUpsertPaths("roles", "address.street").one(mutableUser);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertEquals(user.getRoles(), mutableUser.getRoles());
			assertEquals(user.getAddress().getStreet(), mutableUser.getAddress().getStreet());
			assertNull(user.getAddress().getCity());
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdUpsertMultipleWithExpiry(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address address = new Address();
			address.setCity("city");
			address.setStreet("street");
			mutableUser.setAddress(address);
			couchbaseTemplate.mutateInById(MutableUser.class).withExpiry(Duration.ofSeconds(1)).withUpsertPaths("roles", "address.street").one(mutableUser);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertEquals(user.getRoles(), mutableUser.getRoles());
			assertEquals(user.getAddress().getStreet(), mutableUser.getAddress().getStreet());
			assertNull(user.getAddress().getCity());

			// user should be gone
			int tries = 0;
			MutableUser foundUser;
			do {
				sleepSecs(2);
				foundUser = couchbaseTemplate.findById(MutableUser.class).one(user.getId());
			} while (tries++ < 5 && foundUser != null);
			assertNull(foundUser);
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdReplace(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			couchbaseTemplate.mutateInById(MutableUser.class).withReplacePaths("firstname").one(mutableUser);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertEquals(mutableUser.getFirstname(), user.getFirstname());
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdReplaceWithCAS(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setVersion(999);
			
			// if case is incorrect then exception is thrown
			assertThrows(OptimisticLockingFailureException.class, ()-> couchbaseTemplate.mutateInById(MutableUser.class).withCasProvided()
					.withReplacePaths("firstname").one(mutableUser));
			
			// success on correct cas
			mutableUser.setVersion(user.getVersion());
			couchbaseTemplate.mutateInById(MutableUser.class).withCasProvided().withReplacePaths("firstname").one(mutableUser);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertEquals(mutableUser.getFirstname(), user.getFirstname());
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
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
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address mutableAddress = new Address();
			mutableAddress.setCity("othercity");
			mutableUser.setAddress(mutableAddress);
			couchbaseTemplate.mutateInById(MutableUser.class).withReplacePaths("firstname", "address.city").one(mutableUser);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertEquals(mutableUser.getFirstname(), user.getFirstname());
			assertEquals(user.getAddress().getCity(), mutableUser.getAddress().getCity());
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
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
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address mutableAddress = new Address();
			mutableAddress.setCity("othercity");
			mutableUser.setAddress(mutableAddress);
			couchbaseTemplate.mutateInById(MutableUser.class).withDurability(DurabilityLevel.MAJORITY).withReplacePaths("firstname", "address.city").one(mutableUser);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertEquals(mutableUser.getFirstname(), user.getFirstname());
			assertEquals(user.getAddress().getCity(), mutableUser.getAddress().getCity());
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdReplaceMultipleWithExpiry(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			Address address = new Address();
			address.setCity("city");
			user.setAddress(address);
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address mutableAddress = new Address();
			mutableAddress.setCity("othercity");
			mutableAddress.setStreet("street");
			mutableUser.setAddress(mutableAddress);
			couchbaseTemplate.mutateInById(MutableUser.class).withExpiry(Duration.ofSeconds(1)).withReplacePaths("firstname", "address.city").one(mutableUser);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertEquals(mutableUser.getFirstname(), user.getFirstname());
			assertEquals(user.getAddress().getCity(), mutableUser.getAddress().getCity());
			assertNull(user.getAddress().getStreet());

			// user should be gone
			int tries = 0;
			MutableUser foundUser;
			do {
				sleepSecs(2);
				foundUser = couchbaseTemplate.findById(MutableUser.class).one(user.getId());
			} while (tries++ < 5 && foundUser != null);
			assertNull(foundUser);
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdReplaceMissingPath(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address mutableAddress = new Address();
			mutableAddress.setCity("othercity");
			mutableAddress.setStreet("street");
			mutableUser.setAddress(mutableAddress);
			
			assertThrows(InvalidDataAccessResourceUsageException.class, ()-> couchbaseTemplate.mutateInById(MutableUser.class).withReplacePaths("roles", "address.city").one(mutableUser));
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdInsert(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			couchbaseTemplate.mutateInById(MutableUser.class).withInsertPaths("roles").one(mutableUser);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertEquals(mutableUser.getRoles(), user.getRoles());
			assertNotEquals(mutableUser.getFirstname(), user.getFirstname());
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdInsertWithCAS(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			mutableUser.setVersion(999);

			// if case is incorrect then exception is thrown
			assertThrows(OptimisticLockingFailureException.class, ()-> couchbaseTemplate.mutateInById(MutableUser.class).withCasProvided()
					.withInsertPaths("roles").one(mutableUser));

			// success on correct cas
			mutableUser.setVersion(user.getVersion());
			couchbaseTemplate.mutateInById(MutableUser.class).withCasProvided().withInsertPaths("roles").one(mutableUser);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertEquals(mutableUser.getRoles(), user.getRoles());
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdInsertMultiple() {
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address mutableAddress = new Address();
			mutableAddress.setCity("othercity");
			mutableAddress.setStreet("street");
			mutableUser.setAddress(mutableAddress);
			couchbaseTemplate.mutateInById(MutableUser.class).withInsertPaths("roles", "address.city").one(mutableUser);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertEquals(mutableUser.getRoles(), user.getRoles());
			assertEquals(user.getAddress().getCity(), mutableUser.getAddress().getCity());
			assertNull(user.getAddress().getStreet());
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdInsertMultipleWithDurability(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address mutableAddress = new Address();
			mutableAddress.setCity("othercity");
			mutableAddress.setStreet("street");
			mutableUser.setAddress(mutableAddress);
			couchbaseTemplate.mutateInById(MutableUser.class).withDurability(DurabilityLevel.MAJORITY)
					.withInsertPaths("address.city").one(mutableUser);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertEquals(user.getAddress().getCity(), mutableUser.getAddress().getCity());
			assertNull(user.getAddress().getStreet());
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdInsertMultipleWithExpiry(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address mutableAddress = new Address();
			mutableAddress.setCity("othercity");
			mutableAddress.setStreet("street");
			mutableUser.setAddress(mutableAddress);
			
			couchbaseTemplate.mutateInById(MutableUser.class).withExpiry(Duration.ofSeconds(1))
					.withInsertPaths("address.city").one(mutableUser);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertEquals(user.getAddress().getCity(), mutableUser.getAddress().getCity());
			assertNull(user.getAddress().getStreet());

			// user should be gone
			int tries = 0;
			MutableUser foundUser;
			do {
				sleepSecs(2);
				foundUser = couchbaseTemplate.findById(MutableUser.class).one(user.getId());
			} while (tries++ < 5 && foundUser != null);
			assertNull(foundUser);
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdInsertMissingPath(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");

			assertThrows(InvalidDataAccessResourceUsageException.class, ()-> couchbaseTemplate.mutateInById(MutableUser.class).withInsertPaths("firstname").one(mutableUser));
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdRemove(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user.setRoles(Collections.singletonList("ADMIN"));
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			assertNotNull(user.getRoles());

			couchbaseTemplate.mutateInById(MutableUser.class).withRemovePaths("roles").one(user);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertNull(user.getRoles());
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdRemoveWithCAS(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user.setRoles(Collections.singletonList("ADMIN"));
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);
			String userId = user.getId();
			
			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "otherlast");
			mutableUser.setVersion(999);
			// if case is incorrect then exception is thrown
			assertThrows(OptimisticLockingFailureException.class, ()-> couchbaseTemplate.mutateInById(MutableUser.class).withCasProvided()
					.withRemovePaths("roles").one(mutableUser));

			// success on correct cas
			user.setVersion(user.getVersion());
			couchbaseTemplate.mutateInById(MutableUser.class).withCasProvided().withRemovePaths("roles").one(user);

			user = couchbaseTemplate.findById(MutableUser.class).one(userId);

			assertNull(user.getRoles());
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
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
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			couchbaseTemplate.mutateInById(MutableUser.class).withRemovePaths("roles", "address.city").one(user);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertNull(user.getRoles());
			assertNull(user.getAddress().getCity());
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdRemoveMultipleWithDurability(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			couchbaseTemplate.mutateInById(MutableUser.class).withDurability(DurabilityLevel.MAJORITY)
					.withRemovePaths("lastname").one(user);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());

			assertNull(user.getLastname());
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdRemoveMultipleWithExpiry(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			couchbaseTemplate.mutateInById(MutableUser.class).withExpiry(Duration.ofSeconds(1))
					.withRemovePaths("lastname").one(user);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());
			assertNull(user.getLastname());

			// user should be gone
			int tries = 0;
			MutableUser foundUser;
			do {
				sleepSecs(2);
				foundUser = couchbaseTemplate.findById(MutableUser.class).one(user.getId());
			} while (tries++ < 5 && foundUser != null);
			assertNull(foundUser);
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void mutateInByIdRemoveMissingPath(){
		try {
			MutableUser user = new MutableUser(UUID.randomUUID().toString(), "firstname", "lastname");
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");

			assertThrows(InvalidDataAccessResourceUsageException.class, ()-> couchbaseTemplate.mutateInById(MutableUser.class)
					.withRemovePaths("roles").one(mutableUser));
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
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
			user = couchbaseTemplate.insertById(MutableUser.class).one(user);

			MutableUser mutableUser = new MutableUser(user.getId(), "othername", "");
			mutableUser.setRoles(Collections.singletonList("ADMIN"));
			Address mAddress = new Address();
			mAddress.setCity("othercity");
			mAddress.setStreet("otherstreet");
			mutableUser.setAddress(mAddress);
			mutableUser.setSubuser(new MutableUser("subuser", "subfirstname", "sublastname"));

			couchbaseTemplate.mutateInById(MutableUser.class)
					.withInsertPaths("roles", "subuser.firstname")
					.withRemovePaths("address.city")
					.withUpsertPaths("firstname")
					.withReplacePaths("address.street")
					.one(mutableUser);

			user = couchbaseTemplate.findById(MutableUser.class).one(user.getId());
			
			assertNull(user.getAddress().getCity());
			assertEquals(mAddress.getStreet(),  user.getAddress().getStreet());
			assertEquals(mutableUser.getRoles(),  user.getRoles());
			assertEquals(mutableUser.getFirstname(),  user.getFirstname());
			assertEquals(mutableUser.getSubuser().getFirstname(),  user.getSubuser().getFirstname());
			assertNull(user.getSubuser().getLastname());
		} finally {
			couchbaseTemplate.removeByQuery(MutableUser.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		}
	}

	@Test
	void insertById() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User inserted = couchbaseTemplate.insertById(User.class).one(user);
		assertEquals(user, inserted);
		User found = couchbaseTemplate.findById(User.class).one(user.getId());
		assertEquals(inserted, found);
		assertThrows(DuplicateKeyException.class, () -> couchbaseTemplate.insertById(User.class).one(user));
		couchbaseTemplate.removeById(User.class).one(user.getId());
	}

	@Test
	void insertByIdwithDurability() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User inserted = null;

		// occasionally gives "reactor.core.Exceptions$OverflowException: Could not emit value due to lack of requests"
		for (int i = 1; i != 5; i++) {
			try {
				inserted = couchbaseTemplate.insertById(User.class).withDurability(PersistTo.ACTIVE, ReplicateTo.NONE)
						.one(user);
				break;
			} catch (Exception ofe) {
				System.out.println("" + i + " caught: " + ofe);
				couchbaseTemplate.removeByQuery(User.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
				if (i == 4) {
					throw ofe;
				}
				sleepSecs(1);
			}
		}
		assertEquals(user, inserted);
		assertThrows(DuplicateKeyException.class, () -> couchbaseTemplate.insertById(User.class).one(user));
		couchbaseTemplate.removeById(User.class).one(user.getId());
	}

	@Test
	void insertByIdWithAnnotatedDurability() {
		UserAnnotatedPersistTo user = new UserAnnotatedPersistTo(UUID.randomUUID().toString(), "firstname", "lastname");
		UserAnnotatedPersistTo inserted = null;

		// occasionally gives "reactor.core.Exceptions$OverflowException: Could not emit value due to lack of requests"
		for (int i = 1; i != 5; i++) {
			try {
				inserted = couchbaseTemplate.insertById(UserAnnotatedPersistTo.class)
						.one(user);
				break;
			} catch (Exception ofe) {
				System.out.println("" + i + " caught: " + ofe);
				couchbaseTemplate.removeByQuery(UserAnnotatedPersistTo.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
				if (i == 4) {
					throw ofe;
				}
				sleepSecs(1);
			}
		}
		assertEquals(user, inserted);
		assertThrows(DuplicateKeyException.class, () -> couchbaseTemplate.insertById(UserAnnotatedPersistTo.class).one(user));
		couchbaseTemplate.removeById(UserAnnotatedPersistTo.class).one(user.getId());
	}

	@Test
	void insertByIdWithAnnotatedDurability2() {
		UserAnnotatedDurability user = new UserAnnotatedDurability(UUID.randomUUID().toString(), "firstname", "lastname");
		UserAnnotatedDurability inserted = null;

		// occasionally gives "reactor.core.Exceptions$OverflowException: Could not emit value due to lack of requests"
		for (int i = 1; i != 5; i++) {
			try {
				inserted = couchbaseTemplate.insertById(UserAnnotatedDurability.class)
						.one(user);
				break;
			} catch (Exception ofe) {
				System.out.println("" + i + " caught: " + ofe);
				couchbaseTemplate.removeByQuery(UserAnnotatedDurability.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
				if (i == 4) {
					throw ofe;
				}
				sleepSecs(1);
			}
		}
		assertEquals(user, inserted);
		assertThrows(DuplicateKeyException.class, () -> couchbaseTemplate.insertById(UserAnnotatedDurability.class).one(user));
		couchbaseTemplate.removeById(UserAnnotatedDurability.class).one(user.getId());
	}

	@Test
	void insertByIdWithAnnotatedDurabilityExpression() {
		UserAnnotatedDurabilityExpression user = new UserAnnotatedDurabilityExpression(UUID.randomUUID().toString(), "firstname", "lastname");
		UserAnnotatedDurabilityExpression inserted = null;

		// occasionally gives "reactor.core.Exceptions$OverflowException: Could not emit value due to lack of requests"
		for (int i = 1; i != 5; i++) {
			try {
				inserted = couchbaseTemplate.insertById(UserAnnotatedDurabilityExpression.class)
						.one(user);
				break;
			} catch (Exception ofe) {
				System.out.println("" + i + " caught: " + ofe);
				couchbaseTemplate.removeByQuery(UserAnnotatedDurabilityExpression.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
				if (i == 4) {
					throw ofe;
				}
				sleepSecs(1);
			}
		}
		assertEquals(user, inserted);
		assertThrows(DuplicateKeyException.class, () -> couchbaseTemplate.insertById(UserAnnotatedDurabilityExpression.class).one(user));
		couchbaseTemplate.removeById(UserAnnotatedDurabilityExpression.class).one(user.getId());
	}

	@Test
	void existsById() {
		String id = UUID.randomUUID().toString();
		assertFalse(couchbaseTemplate.existsById().one(id));

		User user = new User(id, "firstname", "lastname");
		User inserted = couchbaseTemplate.insertById(User.class).one(user);
		assertEquals(user, inserted);
		assertTrue(couchbaseTemplate.existsById().one(id));
		couchbaseTemplate.removeById(User.class).one(user.getId());
	}

	@Test
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void saveAndFindImmutableById() {
		PersonValue personValue = new PersonValue(UUID.randomUUID().toString(), 123, "408", "l");
		PersonValue inserted = null;
		PersonValue upserted = null;
		PersonValue replaced = null;

		inserted = couchbaseTemplate.insertById(PersonValue.class).one(personValue);
		assertNotEquals(0, inserted.getVersion());
		PersonValue foundInserted = couchbaseTemplate.findById(PersonValue.class).one(inserted.getId());
		assertNotNull(foundInserted, "inserted personValue not found");
		assertEquals(inserted, foundInserted);

		// upsert will insert
		couchbaseTemplate.removeById().one(inserted.getId());
		upserted = couchbaseTemplate.upsertById(PersonValue.class).one(inserted);
		assertNotEquals(0, upserted.getVersion());
		PersonValue foundUpserted = couchbaseTemplate.findById(PersonValue.class).one(upserted.getId());
		assertNotNull(foundUpserted, "upserted personValue not found");
		assertEquals(upserted, foundUpserted);

		// upsert will replace
		upserted = couchbaseTemplate.upsertById(PersonValue.class).one(inserted);
		assertNotEquals(0, upserted.getVersion());
		PersonValue foundUpserted2 = couchbaseTemplate.findById(PersonValue.class).one(upserted.getId());
		assertNotNull(foundUpserted2, "upserted personValue not found");
		assertEquals(upserted, foundUpserted2);

		replaced = couchbaseTemplate.replaceById(PersonValue.class).one(upserted);
		assertNotEquals(0, replaced.getVersion());
		PersonValue foundReplaced = couchbaseTemplate.findById(PersonValue.class).one(replaced.getId());
		assertNotNull(foundReplaced, "replaced personValue not found");
		assertEquals(replaced, foundReplaced);
		couchbaseTemplate.removeById(PersonValue.class).one(replaced.getId());
	}

	@Test
	void rangeScan() {
		String id = "A";
		String lower = null;
		String upper = null;
		for (int i = 0; i < 10; i++) {
			if (lower == null) {
				lower = "" + i;
			}
			User inserted = couchbaseTemplate.insertById(User.class).one(new User("" + i, "fn_" + i, "ln_" + i));
			upper = "" + i;
		}
		MutationToken mt = couchbaseTemplate.getCouchbaseClientFactory().getDefaultCollection()
				.upsert(id, JsonObject.create().put("id", id)).mutationToken().get();
    Stream<User> users = couchbaseTemplate.rangeScan(User.class).consistentWith(MutationState.from(mt))
        /*.withSort(ScanSort.ASCENDING)*/.rangeScan(lower, upper);
		for (User u : users.toList()) {
			System.err.print(u);
			System.err.println(",");
			assertTrue(u.getId().compareTo(lower) >= 0 && u.getId().compareTo(upper) <= 0);
			couchbaseTemplate.removeById(User.class).one(u.getId());
		}
		couchbaseTemplate.getCouchbaseClientFactory().getDefaultCollection().remove(id);
	}

	@Test
	void rangeScanId() {
		String id = "A";
		String lower = null;
		String upper = null;
		for (int i = 0; i < 10; i++) {
			if (lower == null) {
				lower = "" + i;
			}
			User inserted = couchbaseTemplate.insertById(User.class).one(new User("" + i, "fn_" + i, "ln_" + i));
			upper = "" + i;
		}
		MutationToken mt = couchbaseTemplate.getCouchbaseClientFactory().getDefaultCollection()
				.upsert(id, JsonObject.create().put("id", id)).mutationToken().get();
		Stream<String> userIds = couchbaseTemplate.rangeScan(User.class).consistentWith(MutationState.from(mt))
        /*.withSort(ScanSort.ASCENDING)*/.rangeScanIds(lower, upper);
		for (String userId : userIds.toList()) {
			System.err.print(userId);
			System.err.println(",");
			assertTrue(userId.compareTo(lower) >= 0 && userId.compareTo(upper) <= 0);
			couchbaseTemplate.removeById(User.class).one(userId);
		}
		couchbaseTemplate.getCouchbaseClientFactory().getDefaultCollection().remove(id);

	}

	@Test
    @Disabled // it's finding _txn documents with source = a single 0 byte which fails to deserialize
	void sampleScan() {
		String id = "A";
		String lower = null;
		String upper = null;
		for (int i = 0; i < 10; i++) {
			if (lower == null) {
				lower = "" + i;
			}
			User inserted = couchbaseTemplate.insertById(User.class).one(new User("" + i, "fn_" + i, "ln_" + i));
			upper = "" + i;
		}
		MutationToken mt = couchbaseTemplate.getCouchbaseClientFactory().getDefaultCollection()
			.upsert(id, JsonObject.create().put("id", id)).mutationToken().get();
    Stream<User> users = couchbaseTemplate.rangeScan(User.class).consistentWith(MutationState.from(mt))
        /*.withSort(ScanSort.ASCENDING)*/.samplingScan(5l, (Long)null);
    List<User> usersList = users.toList();
		assertEquals(5, usersList.size(), "number in sample");
    for (User u : usersList) {
			System.err.print(u);
			System.err.println(",");
			// assertTrue(u.getId().compareTo(lower) >= 0 && u.getId().compareTo(upper) <= 0);
			//couchbaseTemplate.removeById(User.class).one(u.getId());
		}
		couchbaseTemplate.getCouchbaseClientFactory().getDefaultCollection().remove(id);
	}

	@Test
	void sampleScanId() {
		String id = "A";
		String lower = null;
		String upper = null;
		for (int i = 0; i < 10; i++) {
			if (lower == null) {
				lower = "" + i;
			}
			User inserted = couchbaseTemplate.insertById(User.class).one(new User("" + i, "fn_" + i, "ln_" + i));
			upper = "" + i;
		}
		MutationToken mt = couchbaseTemplate.getCouchbaseClientFactory().getDefaultCollection()
			.upsert(id, JsonObject.create().put("id", id)).mutationToken().get();
		Stream<String> userIds = couchbaseTemplate.rangeScan(User.class).consistentWith(MutationState.from(mt))
        /*.withSort(ScanSort.ASCENDING)*/.samplingScanIds(5l);
		for (String userId : userIds.toList()) {
			System.err.print(userId);
			System.err.println(",");
			//assertTrue(userId.compareTo(lower) >= 0 && userId.compareTo(upper) <= 0);
			//couchbaseTemplate.removeById(User.class).one(userId);
		}
		couchbaseTemplate.getCouchbaseClientFactory().getDefaultCollection().remove(id);

	}

	private void sleepSecs(int i) {
		try {
			Thread.sleep(i * 1000);
		} catch (InterruptedException ie) {}
	}
}
