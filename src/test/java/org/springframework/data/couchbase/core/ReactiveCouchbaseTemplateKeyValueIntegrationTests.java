/*
 * Copyright 2012-2020 the original author or authors
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.couchbase.core.ExecutableRemoveByIdOperation.ExecutableRemoveById;
import org.springframework.data.couchbase.core.support.OneAndAllEntityReactive;
import org.springframework.data.couchbase.domain.NaiveAuditorAware;
import org.springframework.data.couchbase.domain.PersonValue;
import org.springframework.data.couchbase.domain.ReactiveNaiveAuditorAware;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserAnnotated;
import org.springframework.data.couchbase.domain.UserAnnotated2;
import org.springframework.data.couchbase.domain.UserAnnotated3;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;

import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

/**
 * KV tests Theses tests rely on a cb server running.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
class ReactiveCouchbaseTemplateKeyValueIntegrationTests extends JavaIntegrationTests {

	@BeforeEach
	@Override
	public void beforeEach() {
		super.beforeEach();
		reactiveCouchbaseTemplate.removeByQuery(User.class).all();
		reactiveCouchbaseTemplate.removeByQuery(UserAnnotated.class).all();
		reactiveCouchbaseTemplate.removeByQuery(UserAnnotated2.class).all();
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
		if(user == modified){
			throw new RuntimeException ( " user == modified ");
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
		assertThrows(DataIntegrityViolationException.class,
				() -> reactiveCouchbaseTemplate.replaceById(User.class).one(badUser).block());

		User found = reactiveCouchbaseTemplate.findById(User.class).one(user.getId()).block();
		user.setVersion(found.getVersion());
		assertEquals(modified, found);

		reactiveCouchbaseTemplate.removeById().one(user.getId()).block();
	}

	@Test
	void withDurability()
			throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
		Class<?> clazz = User.class; // for now, just User.class. There is no Durability annotation.
		// insert, replace, upsert
		for (OneAndAllEntityReactive<User> operator : new OneAndAllEntityReactive[] {
				reactiveCouchbaseTemplate.insertById(clazz), reactiveCouchbaseTemplate.replaceById(clazz),
				reactiveCouchbaseTemplate.upsertById(clazz) }) {
			// create an entity of type clazz
			Constructor<?> cons = clazz.getConstructor(String.class, String.class, String.class);
			User user = (User) cons.newInstance("" + operator.getClass().getSimpleName() + "_" + clazz.getSimpleName(),
					"firstname", "lastname");

			if (clazz.equals(User.class)) { // User.java doesn't have an durability annotation
				operator = (OneAndAllEntityReactive<User>) ((WithDurability<User>) operator).withDurability(PersistTo.ACTIVE,
						ReplicateTo.NONE);
			}

			// if replace, we need to insert a document to replace
			if (operator instanceof ReactiveReplaceByIdOperation.ReactiveReplaceById) {
				reactiveCouchbaseTemplate.insertById(User.class).one(user).block();
			}
			// call to insert/replace/update
			User returned = operator.one(user).block();
			assertEquals(user, returned);
			User found = reactiveCouchbaseTemplate.findById(User.class).one(user.getId()).block();
			assertEquals(user, found);

			if (operator instanceof ReactiveReplaceByIdOperation.ReactiveReplaceById) {
				reactiveCouchbaseTemplate.removeById().withDurability(PersistTo.ACTIVE, ReplicateTo.NONE).one(user.getId())
						.block();
				User removed = (User) reactiveCouchbaseTemplate.findById(user.getClass()).one(user.getId()).block();
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
			for (OneAndAllEntityReactive<User> operator : new OneAndAllEntityReactive[] {
					reactiveCouchbaseTemplate.insertById(clazz), reactiveCouchbaseTemplate.replaceById(clazz),
					reactiveCouchbaseTemplate.upsertById(clazz) }) {

				// create an entity of type clazz
				Constructor<?> cons = clazz.getConstructor(String.class, String.class, String.class);
				User user = (User) cons.newInstance("" + operator.getClass().getSimpleName() + "_" + clazz.getSimpleName(),
						"firstname", "lastname");

				if (clazz.equals(User.class)) { // User.java doesn't have an expiry annotation
					operator = (OneAndAllEntityReactive<User>) ((WithExpiry<User>) operator).withExpiry(Duration.ofSeconds(1));
				} else if (clazz.equals(UserAnnotated3.class)) { // override the expiry from the annotation with no expiry
					operator = (OneAndAllEntityReactive<User>) ((WithExpiry<User>) operator).withExpiry(Duration.ofSeconds(0));
				}

				// if replace or remove, we need to insert a document to replace
				if (operator instanceof ReactiveReplaceByIdOperation.ReactiveReplaceById
						|| operator instanceof ExecutableRemoveById) {
					reactiveCouchbaseTemplate.insertById(User.class).one(user).block();
				}
				// call to insert/replace/update
				User returned = operator.one(user).block();
				assertEquals(user, returned);
				users.add(user);
			}
		}
		// check that they are gone after a few seconds.
		sleepSecs(4);
		for (User user : users) {
			User found = reactiveCouchbaseTemplate.findById(user.getClass()).one(user.getId()).block();
			if (found instanceof UserAnnotated3) {
				assertNotNull(found, "found should be non null as it was set to have no expirty");
			} else {
				assertNull(found, "found should have been null as document should be expired");
			}
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
			assertThrows(DataIntegrityViolationException.class, () -> reactiveCouchbaseTemplate.removeById()
					.withCas(reactiveCouchbaseTemplate.support().getCas(modified)).one(modified.getId()).block());
			modified.setVersion(savedCas);
			reactiveCouchbaseTemplate.removeById().withCas(reactiveCouchbaseTemplate.support().getCas(modified))
					.one(modified.getId()).block();
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
		PersonValue personValue = new PersonValue(UUID.randomUUID().toString(), 123, "f", "l");
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

	}

	private void sleepSecs(int i) {
		try {
			Thread.sleep(i * 1000);
		} catch (InterruptedException ie) {}
	}

}
