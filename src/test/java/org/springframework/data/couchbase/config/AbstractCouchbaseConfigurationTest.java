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

package org.springframework.data.couchbase.config;

import com.couchbase.client.CouchbaseClient;
import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.couchbase.TestApplicationConfig;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Unit test for {@link AbstractCouchbaseConfiguration}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestApplicationConfig.class)
public class AbstractCouchbaseConfigurationTest {

  @Autowired
  private CouchbaseClient client;

  @Test
  public void usesConfigClassPackageAsBaseMappingPackage() throws Exception {
    AbstractCouchbaseConfiguration config = new SampleCouchbaseConfiguration();

    assertEquals(config.getMappingBasePackage(),
      SampleCouchbaseConfiguration.class.getPackage().getName());
    assertEquals(config.getInitialEntitySet().size(), 1);
    assertTrue(config.getInitialEntitySet().contains(Entity.class));
  }

  class SampleCouchbaseConfiguration extends AbstractCouchbaseConfiguration {
    @Bean
    @Override
    public CouchbaseClient couchbaseClient() throws Exception {
      return client;
    }
  }

  @Document
  static class Entity {
  }
}
