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
import static org.springframework.data.couchbase.config.BeanNames.COUCHBASE_TEMPLATE;
import static org.springframework.data.couchbase.config.BeanNames.REACTIVE_COUCHBASE_TEMPLATE;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.SimpleCouchbaseClientFactory;
import org.springframework.data.couchbase.core.ExecutableReplaceByIdOperation.ExecutableReplaceById;
import org.springframework.data.couchbase.core.ExecutableRemoveByIdOperation.ExecutableRemoveById;

import org.springframework.data.couchbase.domain.Config;
import org.springframework.data.couchbase.domain.PersonValue;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserAnnotated;
import org.springframework.data.couchbase.domain.UserAnnotated2;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;

import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

/**
 * KV tests Theses tests rely on a cb server running.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
class CouchbaseTemplateKeyValueIntegrationTests extends ClusterAwareIntegrationTests {

	private static CouchbaseClientFactory couchbaseClientFactory;
	private CouchbaseTemplate couchbaseTemplate;
	private ReactiveCouchbaseTemplate reactiveCouchbaseTemplate;

	@BeforeAll
	static void beforeAll() {
		couchbaseClientFactory = new SimpleCouchbaseClientFactory(connectionString(), authenticator(), bucketName());
		couchbaseClientFactory.getBucket().waitUntilReady(Duration.ofSeconds(10));
		couchbaseClientFactory.getCluster().queryIndexes().createPrimaryIndex(bucketName(),
				CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions().ignoreIfExists(true));
	}

	@AfterAll
	static void afterAll() throws IOException {
		couchbaseClientFactory.close();
	}

	@BeforeEach
	void beforeEach() {
		ApplicationContext ac = new AnnotationConfigApplicationContext(Config.class);
		couchbaseTemplate = (CouchbaseTemplate) ac.getBean(COUCHBASE_TEMPLATE);
		reactiveCouchbaseTemplate = (ReactiveCouchbaseTemplate) ac.getBean(REACTIVE_COUCHBASE_TEMPLATE);
		couchbaseTemplate.removeByQuery(User.class).all();
		couchbaseTemplate.removeByQuery(UserAnnotated.class).all();
		couchbaseTemplate.removeByQuery(UserAnnotated2.class).all();
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
		assertEquals(user, found);

		couchbaseTemplate.removeById().one(user.getId());
		reactiveCouchbaseTemplate.replaceById(User.class).withDurability(PersistTo.ACTIVE, ReplicateTo.THREE).one(user);
	}

	@Test
	void withDurability()
			throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
		Class clazz = User.class; // for now, just User.class. There is no Durability annotation.
		// insert, replace, upsert
		for (OneAndAll<User> operator : new OneAndAll[] { couchbaseTemplate.insertById(clazz),
				couchbaseTemplate.replaceById(clazz), couchbaseTemplate.upsertById(clazz)}) {
			// create an entity of type clazz
			Constructor cons = clazz.getConstructor(String.class, String.class, String.class);
			User user = (User) cons.newInstance("" + operator.getClass().getSimpleName() + "_" + clazz.getSimpleName(),
					"firstname", "lastname");

			if (clazz.equals(User.class)) { // User.java doesn't have an durability annotation
				operator = (OneAndAll) ((WithDurability<User>) operator).withDurability(PersistTo.ACTIVE, ReplicateTo.NONE);
			}

			// if replace, we need to insert a document to replace
			if (operator instanceof ExecutableReplaceById) {
				couchbaseTemplate.insertById(User.class).one(user);
			}
			// call to insert/replace/update
			User returned = (User)operator.one(user);
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
		for (Class clazz : new Class[] { User.class, UserAnnotated.class, UserAnnotated2.class }) {
			// insert, replace, upsert
			for (OneAndAll<User> operator : new OneAndAll[] { couchbaseTemplate.insertById(clazz),
					couchbaseTemplate.replaceById(clazz), couchbaseTemplate.upsertById(clazz) }) {

				// create an entity of type clazz
				Constructor cons = clazz.getConstructor(String.class, String.class, String.class);
				User user = (User) cons.newInstance("" + operator.getClass().getSimpleName() + "_" + clazz.getSimpleName(),
						"firstname", "lastname");

				if (clazz.equals(User.class)) { // User.java doesn't have an expiry annotation
					operator = (OneAndAll) ((WithExpiry<User>) operator).withExpiry(Duration.ofSeconds(1));
				}

				// if replace or remove, we need to insert a document to replace
				if (operator instanceof ExecutableReplaceById || operator instanceof ExecutableRemoveById) {
					couchbaseTemplate.insertById(User.class).one(user);
				}
				// call to insert/replace/update
				User returned = (User)operator.one(user);
				assertEquals(user, returned);
				users.add(user);
			}
		}
		// check that they are gone after a few seconds.
		sleepSecs(4);
		for (User user : users) {
			User found = (User) couchbaseTemplate.findById(user.getClass()).one(user.getId());
			assertNull(found, "found should have been null as document should be expired");
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
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User modified = couchbaseTemplate.upsertById(User.class).one(user);
		assertEquals(user, modified);

		RemoveResult removeResult = couchbaseTemplate.removeById().one(user.getId());
		assertEquals(user.getId(), removeResult.getId());
		assertTrue(removeResult.getCas() != 0);
		assertTrue(removeResult.getMutationToken().isPresent());

		assertNull(couchbaseTemplate.findById(User.class).one(user.getId()));
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
		User inserted = couchbaseTemplate.insertById(User.class).withDurability(PersistTo.ACTIVE, ReplicateTo.NONE)
				.one(user);
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
