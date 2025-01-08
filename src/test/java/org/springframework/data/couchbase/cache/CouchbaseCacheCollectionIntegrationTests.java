/*
 * Copyright 2022-2025 the original author or authors
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

package org.springframework.data.couchbase.cache;

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.CollectionAwareIntegrationTests;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.data.couchbase.domain.Config;

/**
 * CouchbaseCache tests Theses tests rely on a cb server running.
 *
 * @author Michael Reiche
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED, missesCapabilities = { Capabilities.COLLECTIONS })
@SpringJUnitConfig(Config.class)
@DirtiesContext
class CouchbaseCacheCollectionIntegrationTests extends CollectionAwareIntegrationTests {

	volatile CouchbaseCache cache;

    @Autowired CouchbaseTemplate couchbaseTemplate;

	@BeforeEach
	@Override
	public void beforeEach() {
		super.beforeEach();
		cache = CouchbaseCacheManager.create(couchbaseTemplate.getCouchbaseClientFactory()).createCouchbaseCache("myCache",
				CouchbaseCacheConfiguration.defaultCacheConfig().collection("my_collection"));
		cache.clear();
	}

	@Test
	void cachePutGet() {
		CacheUser user1 = new CacheUser(UUID.randomUUID().toString(), "first1", "last1");
		CacheUser user2 = new CacheUser(UUID.randomUUID().toString(), "first2", "last2");
		assertNull(cache.get(user1.getId())); // was not put -> cacheMiss
		cache.put(user1.getId(), user1); // put user1
		cache.put(user2.getId(), user2); // put user2
		assertEquals(user1, cache.get(user1.getId()).get()); // get user1
		assertEquals(user2, cache.get(user2.getId()).get()); // get user2
	}

	@Test
	void cacheGetValueLoaderWithClass() {
		CacheUser user1 = new CacheUser(UUID.randomUUID().toString(), "first1", "last1");
		assertNull(cache.get(user1.getId(), CacheUser.class)); // was not put -> cacheMiss
		assertEquals(user1, cache.get(user1.getId(), () -> user1)); // put and get user1
		assertEquals(user1, cache.get(user1.getId(), () -> user1, CacheUser.class)); // already put, get user1

		CacheUser user2 = new CacheUser(UUID.randomUUID().toString(), "first2", "last2");
		assertNull(cache.get(user2.getId(), CacheUser.class)); // was not put -> cacheMiss
		assertEquals(user2, cache.get(user2.getId(), () -> user2, CacheUser.class)); // put and get user2
		assertEquals(user2, cache.get(user2.getId(), () -> user2, CacheUser.class)); // already put, get user2
	}

	@Test
	void cacheGetValueLoaderNoClass() {
		CacheUser user1 = new CacheUser(UUID.randomUUID().toString(), "first1", "last1");
		assertNull(cache.get(user1.getId())); // was not put -> cacheMiss
		assertEquals(user1, cache.get(user1.getId(), () -> user1)); // put and get user1
		assertEquals(user1, cache.get(user1.getId(), () -> user1)); // already put, get user1

		CacheUser user2 = new CacheUser(UUID.randomUUID().toString(), "first2", "last2");
		assertNull(cache.get(user2.getId())); // was not put -> cacheMiss
		assertEquals(user2, cache.get(user2.getId(), () -> user2)); // put and get user2
		assertEquals(user2, cache.get(user2.getId(), () -> user2)); // already put, get user2
	}

	@Test
	void cacheEvict() {
		CacheUser user1 = new CacheUser(UUID.randomUUID().toString(), "first1", "last1");
		CacheUser user2 = new CacheUser(UUID.randomUUID().toString(), "first2", "last2");
		cache.put(user1.getId(), user1); // put user1
		cache.put(user2.getId(), user2); // put user2
		cache.evict(user1.getId()); // evict user1
		assertNull(cache.get(user1.getId())); // get user1 -> not present
		assertEquals(user2, cache.get(user2.getId()).get()); // get user2 -> present
	}

	@Test
	void cacheClear() {
		CacheUser user1 = new CacheUser(UUID.randomUUID().toString(), "first1", "last1");
		CacheUser user2 = new CacheUser(UUID.randomUUID().toString(), "first2", "last2");
		cache.put(user1.getId(), user1); // put user1
		cache.put(user2.getId(), user2); // put user2
		cache.clear();
		assertNull(cache.get(user1.getId())); // get user1 -> not present
		assertNull(cache.get(user2.getId())); // get user2 -> not present
	}

	@Test
	void cacheHitMiss() {
		CacheUser user1 = new CacheUser(UUID.randomUUID().toString(), "first1", "last1");
		CacheUser user2 = new CacheUser(UUID.randomUUID().toString(), "first2", "last2");
		assertNull(cache.get(user2.getId())); // get user2 -> cacheMiss
		cache.put(user1.getId(), null); // cache a null
		assertNotNull(cache.get(user1.getId())); // cacheHit null
		assertNull(cache.get(user1.getId()).get()); // fetch cached null
	}

	@Test
	void cachePutIfAbsent() {
		CacheUser user1 = new CacheUser(UUID.randomUUID().toString(), "first1", "last1");
		CacheUser user2 = new CacheUser(UUID.randomUUID().toString(), "first2", "last2");
		assertNull(cache.putIfAbsent(user1.getId(), user1)); // should put user1, return null
		assertEquals(user1, cache.putIfAbsent(user1.getId(), user2).get()); // should not put user2, should return user1
		assertEquals(user1, cache.get(user1.getId()).get()); // user1.getId() is still user1
	}

}
