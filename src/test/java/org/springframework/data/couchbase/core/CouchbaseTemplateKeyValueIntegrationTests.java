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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.couchbase.client.java.query.QueryScanConsistency;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.couchbase.core.ExecutableRemoveByIdOperation.ExecutableRemoveById;
import org.springframework.data.couchbase.core.ExecutableReplaceByIdOperation.ExecutableReplaceById;
import org.springframework.data.couchbase.core.support.OneAndAllEntity;
import org.springframework.data.couchbase.domain.PersonValue;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserAnnotated;
import org.springframework.data.couchbase.domain.UserAnnotated2;
import org.springframework.data.couchbase.domain.UserAnnotated3;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;

import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

;

/**
 * KV tests Theses tests rely on a cb server running.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
class CouchbaseTemplateKeyValueIntegrationTests extends JavaIntegrationTests {

	@BeforeEach
	@Override
	public void beforeEach() {
		super.beforeEach();
		couchbaseTemplate.removeByQuery(User.class).all();
		couchbaseTemplate.removeByQuery(UserAnnotated.class).all();
		couchbaseTemplate.removeByQuery(UserAnnotated2.class).all();
		couchbaseTemplate.removeByQuery(UserAnnotated3.class).all();
	}

	@Test
	void upsertAndFindById() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User modified = couchbaseTemplate.upsertById(User.class).one(user);
		assertEquals(user, modified);

		modified = couchbaseTemplate.replaceById(User.class).one(user);
		assertEquals(user, modified);

		user.setVersion(12345678);
		assertThrows(DataIntegrityViolationException.class, () -> couchbaseTemplate.replaceById(User.class).one(user));

		User found = couchbaseTemplate.findById(User.class).one(user.getId());
		user.setVersion(found.getVersion());
		assertEquals(user, found);

		couchbaseTemplate.removeById().one(user.getId());
		reactiveCouchbaseTemplate.replaceById(User.class).withDurability(PersistTo.ACTIVE, ReplicateTo.THREE).one(user);
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
					System.out.println(""+i+" caught: "+ofe);
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
		for (Class clazz : new Class[] { User.class, UserAnnotated.class, UserAnnotated2.class, UserAnnotated3.class }) {
			// insert, replace, upsert
			for (OneAndAllEntity<User> operator : new OneAndAllEntity[] { couchbaseTemplate.insertById(clazz),
					couchbaseTemplate.replaceById(clazz), couchbaseTemplate.upsertById(clazz) }) {

				// create an entity of type clazz
				Constructor cons = clazz.getConstructor(String.class, String.class, String.class);
				User user = (User) cons.newInstance("" + operator.getClass().getSimpleName() + "_" + clazz.getSimpleName(),
						"firstname", "lastname");

				if (clazz.equals(User.class)) { // User.java doesn't have an expiry annotation
					operator = (OneAndAllEntity) ((WithExpiry<User>) operator).withExpiry(Duration.ofSeconds(1));
				} else if (clazz.equals(UserAnnotated3.class)) { // override the expiry from the annotation with no expiry
					operator = (OneAndAllEntity) ((WithExpiry<User>) operator).withExpiry(Duration.ofSeconds(0));
				}

				// if replace or remove, we need to insert a document to replace
				if (operator instanceof ExecutableReplaceById || operator instanceof ExecutableRemoveById) {
					couchbaseTemplate.insertById(User.class).one(user);
				}
				// call to insert/replace/update
				User returned = operator.one(user);
				assertEquals(user, returned);
				users.add(user);
			}
		}
		// check that they are gone after a few seconds.
		sleepSecs(4);
		for (User user : users) {
			User found = couchbaseTemplate.findById(user.getClass()).one(user.getId());
			if (found instanceof UserAnnotated3) {
				assertNotNull(found, "found should be non null as it was set to have no expirty");
			} else {
				assertNull(found, "found should have been null as document should be expired");
			}
		}

	}

	@Test
	void findDocWhichDoesNotExist() {
		assertNull(couchbaseTemplate.findById(User.class).one(UUID.randomUUID().toString()));
	}

	@Test
	void upsertAndReplaceById() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
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
			assertThrows(DataIntegrityViolationException.class, () -> couchbaseTemplate.removeById()
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
				System.out.println(""+i+" caught: "+ofe);
				couchbaseTemplate.removeByQuery(User.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
				if (i == 4) {
					throw ofe;
				}
				sleepSecs(1);
			}
		}
		assertEquals(user, inserted);
		assertThrows(DuplicateKeyException.class, () -> couchbaseTemplate.insertById(User.class).one(user));
	}

	@Test
	void existsById() {
		String id = UUID.randomUUID().toString();
		assertFalse(couchbaseTemplate.existsById().one(id));

		User user = new User(id, "firstname", "lastname");
		User inserted = couchbaseTemplate.insertById(User.class).one(user);
		assertEquals(user, inserted);

		assertTrue(couchbaseTemplate.existsById().one(id));

	}

	@Test
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void saveAndFindImmutableById() {
		PersonValue personValue = new PersonValue(UUID.randomUUID().toString(), 123, "f", "l");
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

	}

	private void sleepSecs(int i) {
		try {
			Thread.sleep(i * 1000);
		} catch (InterruptedException ie) {}
	}

}
