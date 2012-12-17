/**
 * Copyright (C) 2009-2012 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.spring.cache;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.spring.config.TestApplicationConfig;
import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Tests the CouchbaseCache class and verifies its functionality.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestApplicationConfig.class)
public class CouchbaseCacheTest {

  /**
   * Contains a reference to the actual CouchbaseClient.
   */
  @Autowired
  private CouchbaseClient client;

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
