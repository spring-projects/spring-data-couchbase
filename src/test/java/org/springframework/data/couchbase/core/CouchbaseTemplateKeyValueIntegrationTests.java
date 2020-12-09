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
import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.SimpleCouchbaseClientFactory;
import org.springframework.data.couchbase.domain.Config;
import org.springframework.data.couchbase.domain.PersonValue;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserAnnotated;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;

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
class CouchbaseTemplateKeyValueIntegrationTests extends ClusterAwareIntegrationTests {

	private static CouchbaseClientFactory couchbaseClientFactory;
	private CouchbaseTemplate couchbaseTemplate;
	private ReactiveCouchbaseTemplate reactiveCouchbaseTemplate;

	@BeforeAll
	static void beforeAll() {
		couchbaseClientFactory = new SimpleCouchbaseClientFactory(connectionString(), authenticator(), bucketName());
		couchbaseClientFactory.getBucket().waitUntilReady(Duration.ofSeconds(10));
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
	void upsertWithDurability() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User modified = couchbaseTemplate.upsertById(User.class).withDurability(PersistTo.ACTIVE, ReplicateTo.NONE)
				.one(user);
		assertEquals(user, modified);
		User found = couchbaseTemplate.findById(User.class).one(user.getId());
		assertEquals(user, found);
		couchbaseTemplate.removeById().one(user.getId());
	}

	@Test
	void upsertWithExpiry() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		try {
			User modified = couchbaseTemplate.upsertById(User.class).withExpiry(Duration.ofSeconds(1)).one(user);
			assertEquals(user, modified);
			sleepSecs(2);
			User found = couchbaseTemplate.findById(User.class).one(user.getId());
			assertNull(found, "found should have been null as document should be expired");
		} finally {
			try {
				couchbaseTemplate.removeById().one(user.getId());
			} catch (DataRetrievalFailureException e) {
				//
			}
		}
	}

	@Test
	void upsertWithExpiryAnnotation() {
		UserAnnotated user = new UserAnnotated(UUID.randomUUID().toString(), "firstname", "lastname");
		try {
			UserAnnotated modified = couchbaseTemplate.upsertById(UserAnnotated.class).one(user);
			assertEquals(user, modified);
			sleepSecs(6);
			User found = couchbaseTemplate.findById(User.class).one(user.getId());
			assertNull(found, "found should have been null as document should be expired");
		} finally {
			try {
				couchbaseTemplate.removeById().one(user.getId());
			} catch (DataRetrievalFailureException e) {
				//
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
		couchbaseTemplate.removeById().one(user.getId());

	}

	@Test
	void insertByIdwithDurability() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User inserted = couchbaseTemplate.insertById(User.class).withDurability(PersistTo.ACTIVE, ReplicateTo.NONE)
				.one(user);
		assertEquals(user, inserted);

		assertThrows(DuplicateKeyException.class, () -> couchbaseTemplate.insertById(User.class).one(user));
		couchbaseTemplate.removeById().one(user.getId());

	}

	@Test
	void insertByIdwithExpiry() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		try {
			User inserted = couchbaseTemplate.insertById(User.class).withExpiry(Duration.ofSeconds(1)).one(user);
			assertEquals(user, inserted);
			sleepSecs(2);
			User found = couchbaseTemplate.findById(User.class).one(user.getId());
			assertNull(found, "found should have been null as document should be expired");
		} finally {
			try {
				couchbaseTemplate.removeById().one(user.getId());
			} catch (DataRetrievalFailureException e) {
				// ignore
			}
		}

	}

	@Test
	void insertWithExpiryAnnotation() {
		UserAnnotated user = new UserAnnotated(UUID.randomUUID().toString(), "firstname", "lastname");
		try {
			UserAnnotated inserted = couchbaseTemplate.insertById(UserAnnotated.class).one(user);
			assertEquals(user, inserted);
			sleepSecs(6);
			User found = couchbaseTemplate.findById(User.class).one(user.getId());
			assertNull(found, "found should have been null as document should be expired");
		} finally {
			try {
				couchbaseTemplate.removeById().one(user.getId());
			} catch (DataRetrievalFailureException e) {
				// ignore
			}
		}
	}

	@Test
	void existsById() {
		String id = UUID.randomUUID().toString();
		assertFalse(couchbaseTemplate.existsById().one(id));

		User user = new User(id, "firstname", "lastname");
		User inserted = couchbaseTemplate.insertById(User.class).one(user);
		assertEquals(user, inserted);

		assertTrue(couchbaseTemplate.existsById().one(id));
		couchbaseTemplate.removeById().one(user.getId());

	}

	@Test
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void saveAndFindImmutableById() {
		PersonValue personValue = new PersonValue(null, 123, "f", "l");
		System.out.println("personValue: " + personValue);
		// personValue = personValue.withVersion(123);
		PersonValue inserted = null;
		PersonValue upserted = null;
		PersonValue replaced = null;

		try {

			inserted = couchbaseTemplate.insertById(PersonValue.class).one(personValue);
			assertNotEquals(0, inserted.getVersion());
			PersonValue foundInserted = couchbaseTemplate.findById(PersonValue.class).one(inserted.getId());
			assertNotNull(foundInserted, "inserted personValue not found");
			assertEquals(inserted, foundInserted);

			// upsert will be inserted
			couchbaseTemplate.removeById().one(inserted.getId());
			upserted = couchbaseTemplate.upsertById(PersonValue.class).one(inserted);
			assertNotEquals(0, upserted.getVersion());
			PersonValue foundUpserted = couchbaseTemplate.findById(PersonValue.class).one(upserted.getId());
			assertNotNull(foundUpserted, "upserted personValue not found");
			assertEquals(upserted, foundUpserted);

			// upsert will be replaced
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

		} finally {
			couchbaseTemplate.removeById().one(inserted.getId());
		}
	}

	private void sleepSecs(int i) {
		try {
			Thread.sleep(i * 1000);
		} catch (InterruptedException ie) {}
	}

}
