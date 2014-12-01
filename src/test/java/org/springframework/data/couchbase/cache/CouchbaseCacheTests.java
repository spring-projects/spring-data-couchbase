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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.data.couchbase.TestApplicationConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.LegacyDocument;

/**
 * Tests the CouchbaseCache class and verifies its functionality.
 *
 * @author Michael Nitschinger
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestApplicationConfig.class)
public class CouchbaseCacheTests {

  /**
   * Contains a reference to the actual CouchbaseClient.
   */
  @Autowired
  private Bucket client;

  /**
   * Simple name of the cache bucket to create.
   */
  private String cacheName = "test";

  /**
   * Tests the basic Cache construction functionality.
   */
  @Test
  public void testConstruction() {
    CouchbaseCache cache = new CouchbaseCache(cacheName, client);

    assertEquals(cacheName, cache.getName());
    assertEquals(client, cache.getNativeCache());
  }

  /**
   * Verifies set() and get() of cache objects.
   */
  @Test
  public void testGetSet() {
    CouchbaseCache cache = new CouchbaseCache(cacheName, client);

    String key = "couchbase-cache-test";
    String value = "Hello World!";
    cache.put(key, value);

    LegacyDocument stored = client.get(LegacyDocument.create(key));
    assertNotNull(stored);
    assertEquals(value, stored.content());

    ValueWrapper loaded = cache.get(key);
    assertEquals(value, loaded.get());
  }

  @Test
  public void testGetSetWithCast() {
    CouchbaseCache cache = new CouchbaseCache(cacheName, client);

    String key = "couchbase-cache-user";
    User user = new User();
    user.firstname = "Michael";

    cache.put(key, user);

    User loaded = cache.get(key, User.class);
    assertNotNull(loaded);
    assertEquals(user.firstname, loaded.firstname);
  }

  /**
   * Verifies the deletion of cache objects.
   *
   * @throws Exception
   */
  @Test
  public void testEvict() throws Exception {
    CouchbaseCache cache = new CouchbaseCache(cacheName, client);

    String key = "couchbase-cache-test";
    LegacyDocument value = LegacyDocument.create(key, "Hello World!");

    LegacyDocument success = client.upsert(value);
    assertTrue(success != null);

    cache.evict(key);
    Object result = client.get(key);
    assertNull(result);
  }

  /**
   * Putting into cache on the same key not null value, and then null value,
   * results in null object
   */
  @Test
  public void testSettingNullAndGetting() {
    CouchbaseCache cache = new CouchbaseCache(cacheName, client);

    String key = "couchbase-cache-test";
    String value = "Hello World!";

    cache.put(key, value);
    cache.put(key, null);

    assertNull(cache.get(key));
  }

  static class User implements Serializable {
    public String firstname;
  }

}
