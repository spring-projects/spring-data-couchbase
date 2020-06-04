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
import static org.springframework.data.couchbase.config.BeanNames.*;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.SimpleCouchbaseClientFactory;
import org.springframework.data.couchbase.domain.Config;
import org.springframework.data.couchbase.domain.NaiveAuditorAware;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.time.AuditingDateTimeProvider;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;

import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.java.query.QueryScanConsistency;
import reactor.core.publisher.Mono;

/**
 * Query tests Theses tests rely on a cb server running
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
class CouchbaseTemplateQueryIntegrationTests extends ClusterAwareIntegrationTests {

	private static CouchbaseClientFactory couchbaseClientFactory;
	private CouchbaseTemplate couchbaseTemplate;
	private ReactiveCouchbaseTemplate reactiveCouchbaseTemplate;

	@BeforeAll
	static void beforeAll() {
		couchbaseClientFactory = new SimpleCouchbaseClientFactory(connectionString(), authenticator(), bucketName());

		try {
			couchbaseClientFactory.getCluster().queryIndexes().createPrimaryIndex(bucketName());
		} catch (IndexExistsException ex) {
			// ignore, all good.
		}
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
	void findByQuery() {
		try {
			User user1 = new User(UUID.randomUUID().toString(), "user1", "user1");
			User user2 = new User(UUID.randomUUID().toString(), "user2", "user2");

			couchbaseTemplate.upsertById(User.class).all(Arrays.asList(user1, user2));

			final List<User> foundUsers = couchbaseTemplate.findByQuery(User.class)
					.consistentWith(QueryScanConsistency.REQUEST_PLUS).all();

			for (User u : foundUsers) {
				System.out.println(u);
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
			couchbaseTemplate.removeByQuery(User.class).all();
		}

		User usery = couchbaseTemplate.findById(User.class).one("userx");
		assertNull(usery, "usery should be null");
		User userz = reactiveCouchbaseTemplate.findById(User.class).one("userx").block();
		assertNull(userz, "uz should be null");

	}

	@Test
	void removeByQuery() {
		User user1 = new User(UUID.randomUUID().toString(), "user1", "user1");
		User user2 = new User(UUID.randomUUID().toString(), "user2", "user2");

		couchbaseTemplate.upsertById(User.class).all(Arrays.asList(user1, user2));

		assertTrue(couchbaseTemplate.existsById().one(user1.getId()));
		assertTrue(couchbaseTemplate.existsById().one(user2.getId()));

		couchbaseTemplate.removeByQuery(User.class).consistentWith(QueryScanConsistency.REQUEST_PLUS).all();

		assertNull(couchbaseTemplate.findById(User.class).one(user1.getId()));
		assertNull(couchbaseTemplate.findById(User.class).one(user2.getId()));

	}

}
