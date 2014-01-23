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

package org.springframework.data.couchbase.config;

import com.couchbase.client.CouchbaseClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.data.couchbase.TestApplicationConfig;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link AbstractCouchbaseConfiguration}
 *
 * @author Michael Nitschinger
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestApplicationConfig.class)
public class AbstractCouchbaseConfigurationTests {

  /**
   * Contains a reference to the actual CouchbaseClient.
   */
  @Autowired
  private CouchbaseClient client;

  @Test
  public void usesConfigClassPackageAsBaseMappingPackage() throws Exception {
    AbstractCouchbaseConfiguration config = new SampleCouchbaseConfiguration();
    assertEquals(config.getMappingBasePackage(), SampleCouchbaseConfiguration.class.getPackage().getName());
    assertEquals(config.getInitialEntitySet().size(), 1);
    assertTrue(config.getInitialEntitySet().contains(Entity.class));
  }

  class SampleCouchbaseConfiguration extends AbstractCouchbaseConfiguration {

    @Override
    protected List<String> bootstrapHosts() {
      return null;
    }

    @Override
    protected String getBucketName() {
      return null;
    }

    @Override
    protected String getBucketPassword() {
      return null;
    }

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
