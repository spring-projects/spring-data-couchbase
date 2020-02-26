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

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.SimpleCouchbaseClientFactory;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;

class CouchbaseTemplateKeyValueIntegrationTests extends ClusterAwareIntegrationTests {

	private static CouchbaseClientFactory couchbaseClientFactory;
	private CouchbaseTemplate couchbaseTemplate;

	@BeforeAll
	static void beforeAll() {
		couchbaseClientFactory = new SimpleCouchbaseClientFactory(connectionString(), authenticator(), bucketName());
	}

	@AfterAll
	static void afterAll() throws IOException {
		couchbaseClientFactory.close();
	}

	@BeforeEach
	void beforeEach() {
		CouchbaseConverter couchbaseConverter = new MappingCouchbaseConverter();
		couchbaseTemplate = new CouchbaseTemplate(couchbaseClientFactory, couchbaseConverter);
	}

	@Test
	void upsertAndFindById() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User modified = couchbaseTemplate.upsertById(User.class).one(user);
		assertEquals(user, modified);

		User found = couchbaseTemplate.findById(User.class).one(user.getId());
		assertEquals(user, found);
	}

	@Test
	void findDocWhichDoesNotExist() {
		assertThrows(DataRetrievalFailureException.class,
				() -> couchbaseTemplate.findById(User.class).one(UUID.randomUUID().toString()));
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

		assertThrows(DataRetrievalFailureException.class, () -> couchbaseTemplate.findById(User.class).one(user.getId()));
	}

	@Test
	void insertById() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User inserted = couchbaseTemplate.insertById(User.class).one(user);
		assertEquals(user, inserted);

		assertThrows(DuplicateKeyException.class, () -> couchbaseTemplate.insertById(User.class).one(user));
	}

	@Test
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void existsById() {
		String id = UUID.randomUUID().toString();
		assertFalse(couchbaseTemplate.existsById().one(id));

		User user = new User(id, "firstname", "lastname");
		User inserted = couchbaseTemplate.insertById(User.class).one(user);
		assertEquals(user, inserted);

		assertTrue(couchbaseTemplate.existsById().one(id));
	}

}
