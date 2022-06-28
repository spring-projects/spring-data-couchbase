/*
 * Copyright 2012-2022 the original author or authors
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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.couchbase.core.ExecutableFindByIdOperation.ExecutableFindById;
import org.springframework.data.couchbase.core.ExecutableRemoveByIdOperation.ExecutableRemoveById;
import org.springframework.data.couchbase.core.ExecutableReplaceByIdOperation.ExecutableReplaceById;
import org.springframework.data.couchbase.core.support.OneAndAllEntity;
import org.springframework.data.couchbase.core.support.OneAndAllId;
import org.springframework.data.couchbase.core.support.WithDurability;
import org.springframework.data.couchbase.core.support.WithExpiry;
import org.springframework.data.couchbase.domain.Address;
import org.springframework.data.couchbase.domain.Config;
import org.springframework.data.couchbase.domain.NaiveAuditorAware;
import org.springframework.data.couchbase.domain.PersonValue;
import org.springframework.data.couchbase.domain.Submission;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserAnnotated;
import org.springframework.data.couchbase.domain.UserAnnotated2;
import org.springframework.data.couchbase.domain.UserAnnotated3;
import org.springframework.data.couchbase.domain.UserSubmission;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * KV tests Theses tests rely on a cb server running.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(Config.class)
class CouchbaseTemplateKeyValueIntegrationTests extends JavaIntegrationTests {

	@Autowired public CouchbaseTemplate couchbaseTemplate;
	@Autowired public ReactiveCouchbaseTemplate reactiveCouchbaseTemplate;

	@BeforeEach
	@Override
	public void beforeEach() {
		couchbaseTemplate.removeByQuery(User.class).all();
		couchbaseTemplate.removeByQuery(UserAnnotated.class).all();
		couchbaseTemplate.removeByQuery(UserAnnotated2.class).all();
		couchbaseTemplate.removeByQuery(UserAnnotated3.class).all();
		couchbaseTemplate.removeByQuery(User.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
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
			sleepMs(3000);

			Collection<User> foundUsers = (Collection<User>) couchbaseTemplate.findById(User.class)
					.all(Arrays.asList(user1.getId(), user2.getId()));
			assertEquals(1, foundUsers.size(), "should have found exactly 1 user");
			assertEquals(user2, foundUsers.iterator().next());
		} finally {
			couchbaseTemplate.removeByQuery(User.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
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
		Class<?> clazz = User.class; // for now, just User.class. There is no Durability annotation.
		// insert, replace, upsert
		for (OneAndAllEntity<User> operator : new OneAndAllEntity[] { couchbaseTemplate.insertById(clazz),
				couchbaseTemplate.replaceById(clazz), couchbaseTemplate.upsertById(clazz) }) {
			// create an entity of type clazz
			Constructor<?> cons = clazz.getConstructor(String.class, String.class, String.class);
			User user = (User) cons.newInstance("" + operator.getClass().getSimpleName() + "_" + clazz.getSimpleName(),
					"firstname", "lastname");

			if (clazz.equals(User.class)) { // User.java doesn't have an durability annotation
				operator = (OneAndAllEntity) ((WithDurability<User>) operator).withDurability(PersistTo.ACTIVE,
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
				couchbaseTemplate.removeById().withDurability(PersistTo.ACTIVE, ReplicateTo.NONE).one(user.getId());
				User removed = (User) couchbaseTemplate.findById(user.getClass()).one(user.getId());
				assertNull(removed, "found should have been null as document should be removed");
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
		sleepSecs(4);
		List<String> errorList = new LinkedList();
		for (User user : users) {
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
	void insertById() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User inserted = couchbaseTemplate.insertById(User.class).one(user);
		assertEquals(user, inserted);
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

	private void sleepSecs(int i) {
		try {
			Thread.sleep(i * 1000);
		} catch (InterruptedException ie) {}
	}

}
