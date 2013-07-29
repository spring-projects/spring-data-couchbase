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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.data.couchbase.TestApplicationConfig;
import org.springframework.data.couchbase.util.BucketCreationListener;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.net.URI;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Tests the CouchbaseCache class and verifies its functionality.
 *
 * @author Michael Nitschinger
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestApplicationConfig.class)
@TestExecutionListeners(BucketCreationListener.class)
public class CouchbaseCacheTests {

  /**
   * Contains a reference to the actual CouchbaseClient.
   */
  private CouchbaseClient client;

  /**
   * Simple name of the cache bucket to create.
   */
  private String cacheName = "test";

  @Autowired
  private String couchbaseHost;

  @Autowired
  private String couchbaseBucket;

  @Autowired
  private String couchbasePassword;

  @Before
  public void setup() throws Exception {
    client = new CouchbaseClient(Arrays.asList(new URI(couchbaseHost)), couchbaseBucket, couchbasePassword);
  }

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

    String stored = (String) client.get(key);
    assertNotNull(stored);
    assertEquals(value, stored);

    ValueWrapper loaded = cache.get(key);
    assertEquals(value, loaded.get());
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
    String value = "Hello World!";

    Boolean success = client.set(key, 0, value).get();
    assertTrue(success);

    cache.evict(key);
    Object result = client.get(key);
    assertNull(result);
  }

}
