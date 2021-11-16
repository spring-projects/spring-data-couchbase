/*
 * Copyright 2022 the original author or authors
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

import com.couchbase.client.java.query.QueryOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.transactions.CouchbaseTemplateTransaction2IntegrationTests;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.CollectionAwareIntegrationTests;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.UUID;

import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * CouchbaseCache tests Theses tests rely on a cb server running.
 *
 * @author Michael Reiche
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED, missesCapabilities = { Capabilities.COLLECTIONS })
class CouchbaseCacheCollectionIntegrationTests extends CollectionAwareIntegrationTests {

	volatile CouchbaseCache cache;

	@BeforeEach
	@Override
	public void beforeEach() {
		super.beforeEach();
		cache = CouchbaseCacheManager.create(couchbaseTemplate.getCouchbaseClientFactory()).createCouchbaseCache("myCache",
				CouchbaseCacheConfiguration.defaultCacheConfig().collection("my_collection"));
		clear(cache);
	}

	private void clear(CouchbaseCache c) {
		couchbaseTemplate.getCouchbaseClientFactory().getCluster().query("SELECT count(*) from `" + bucketName() + "`",
				QueryOptions.queryOptions().scanConsistency(REQUEST_PLUS));
		c.clear();
		couchbaseTemplate.getCouchbaseClientFactory().getCluster().query("SELECT count(*) from `" + bucketName() + "`",
				QueryOptions.queryOptions().scanConsistency(REQUEST_PLUS));
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
	void cacheEvict() {
		CacheUser user1 = new CacheUser(UUID.randomUUID().toString(), "first1", "last1");
		CacheUser user2 = new CacheUser(UUID.randomUUID().toString(), "first2", "last2");
		cache.put(user1.getId(), user1); // put user1
		cache.put(user2.getId(), user2); // put user2
		cache.evict(user1.getId()); // evict user1
		assertEquals(user2, cache.get(user2.getId()).get()); // get user2 -> present
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
