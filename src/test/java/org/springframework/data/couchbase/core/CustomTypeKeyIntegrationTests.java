/*
 * Copyright 2012-present the original author or authors
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

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.convert.DefaultCouchbaseTypeMapper;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.GetResult;

/**
 * @author Michael Reiche
 */
@SpringJUnitConfig(CustomTypeKeyIntegrationTests.Config.class)
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
@DirtiesContext
public class CustomTypeKeyIntegrationTests extends ClusterAwareIntegrationTests {

	private static final String CUSTOM_TYPE_KEY = "javaClass";

	@Autowired private CouchbaseOperations operations;

	@Autowired private CouchbaseClientFactory clientFactory;

	@Test
	void saveSimpleEntityCorrectlyWithDifferentTypeKey() {
		clientFactory.getBucket().waitUntilReady(Duration.ofSeconds(10));

		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		// When using 'mocked', this call runs fine when the test class is ran by itself,
		// but it times-out when ran together with all the tests under
		// org.springframework.data.couchbase
		User modified = operations.upsertById(User.class).one(user);
		assertEquals(user, modified);

		GetResult getResult = clientFactory.getCollection(null).get(user.getId());
		assertEquals("abstractuser", getResult.contentAsObject().getString(CUSTOM_TYPE_KEY));
		assertFalse(getResult.contentAsObject().containsKey(DefaultCouchbaseTypeMapper.DEFAULT_TYPE_KEY));
		operations.removeById(User.class).one(user.getId());
	}

	static class Config extends org.springframework.data.couchbase.domain.Config {
		@Override
		public String typeKey() {
			return CUSTOM_TYPE_KEY;
		}
	}

}
