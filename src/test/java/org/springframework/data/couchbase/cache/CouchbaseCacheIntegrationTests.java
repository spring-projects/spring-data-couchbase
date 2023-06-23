/*
 * Copyright 2022-2023 the original author or authors
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.domain.Config;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserRepository;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * CouchbaseCache tests Theses tests rely on a cb server running.
 *
 * @author Michael Reiche
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(Config.class)
@DirtiesContext
class CouchbaseCacheIntegrationTests extends JavaIntegrationTests {

	volatile CouchbaseCache cache;
	@Autowired CouchbaseCacheManager cacheManager; // autowired not working
	@Autowired UserRepository userRepository; // autowired not working
    @Autowired CouchbaseTemplate couchbaseTemplate;

	@BeforeEach
	@Override
	public void beforeEach() {
		super.beforeEach();
		cache = CouchbaseCacheManager.create(couchbaseTemplate.getCouchbaseClientFactory()).createCouchbaseCache("myCache",
				CouchbaseCacheConfiguration.defaultCacheConfig());
		cache.clear();
	}

	@AfterEach
	@Override
	public void afterEach() {
		cache.clear();
		super.afterEach();
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
	void cacheable() {
		User user = new User("cache_92", "Dave", "Wilson");
		cacheManager.getCache("mySpringCache").clear();
		userRepository.save(user);
		long t0 = System.currentTimeMillis();
		List<User> users = userRepository.getByFirstname(user.getFirstname());
		assert (System.currentTimeMillis() - t0 > 1000 * 5);
		t0 = System.currentTimeMillis();
		users = userRepository.getByFirstname(user.getFirstname());
		assert (System.currentTimeMillis() - t0 < 100);
		userRepository.delete(user);
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
	void cachePutIfAbsent() {
		CacheUser user1 = new CacheUser(UUID.randomUUID().toString(), "first1", "last1");
		CacheUser user2 = new CacheUser(UUID.randomUUID().toString(), "first2", "last2");
		assertNull(cache.putIfAbsent(user1.getId(), user1)); // should put user1, return null
		assertEquals(user1, cache.putIfAbsent(user1.getId(), user2).get()); // should not put user2, should return user1
		assertEquals(user1, cache.get(user1.getId()).get()); // user1.getId() is still user1
	}

	@Test // this WORKS
	public void clearWithDelayOk() throws InterruptedException {
		cache.put("KEY", "VALUE");
		Thread.sleep(50); // give main index time to update
		cache.clear();
		assertNull(cache.get("KEY"));
	}

	@Test
	public void noOpt() {}
}
