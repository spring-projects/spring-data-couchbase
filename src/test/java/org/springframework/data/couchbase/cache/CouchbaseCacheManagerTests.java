/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.cache;

import com.couchbase.client.CouchbaseClient;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.data.couchbase.TestApplicationConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Verifies the correct functionality of the CouchbaseCacheManager.
 *
 * @author Michael Nitschinger
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestApplicationConfig.class)
public class CouchbaseCacheManagerTests {

  /**
   * Contains a reference to the actual CouchbaseClient.
   */
  @Autowired
  private CouchbaseClient client;

  /**
   * Tests the main functionality of the manager: loading the caches.
   */
  @Test
  public void testCacheInit() {
    HashMap<String, CouchbaseClient> instances =
      new HashMap<String, CouchbaseClient>();
    instances.put("test", client);

    CouchbaseCacheManager manager = new CouchbaseCacheManager(instances);
    manager.afterPropertiesSet();
    
    assertEquals(instances, manager.getClients());
    
    Cache cache = manager.getCache("test");
    
    assertNotNull(cache);
    
    assertEquals(cache.getClass(), CouchbaseCache.class);
    assertEquals(((CouchbaseCache) cache).getName(), "test");
    assertEquals(((CouchbaseCache) cache).getTtl(), 0); // default TTL value
    assertEquals(((CouchbaseCache) cache).getNativeCache(), client);
  }
  
  /**
   * Test cache creation with custom TTL values.
   */
  @Test
  public void testCacheInitWithTtl() {
    HashMap<String, CouchbaseClient> instances = new HashMap<String, CouchbaseClient>();
    instances.put("cache1", client);
    instances.put("cache2", client);
    
    HashMap<String, Integer> ttlConfiguration = new HashMap<String, Integer>();
    ttlConfiguration.put("cache1", 100);
    ttlConfiguration.put("cache2", 200);

    CouchbaseCacheManager manager = new CouchbaseCacheManager(instances, ttlConfiguration);
    manager.afterPropertiesSet();
    
    assertEquals(instances, manager.getClients());
    
    Cache cache1 = manager.getCache("cache1");
    Cache cache2 = manager.getCache("cache2");
    
    assertNotNull(cache1);
    assertNotNull(cache2);
    
    assertEquals(cache1.getClass(), CouchbaseCache.class);
    assertEquals(cache2.getClass(), CouchbaseCache.class);
    
    assertEquals(((CouchbaseCache) cache1).getName(), "cache1");
    assertEquals(((CouchbaseCache) cache2).getName(), "cache2");
    
    assertEquals(((CouchbaseCache) cache1).getTtl(), 100);
    assertEquals(((CouchbaseCache) cache2).getTtl(), 200);
    
    assertEquals(((CouchbaseCache) cache1).getNativeCache(), client);
    assertEquals(((CouchbaseCache) cache2).getNativeCache(), client);
  }

}
