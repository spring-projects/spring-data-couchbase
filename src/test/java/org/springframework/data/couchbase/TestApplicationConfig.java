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

package org.springframework.data.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.WriteResultChecking;

import java.util.Arrays;
import java.util.List;

/**
 * @author Michael Nitschinger
 */
@Configuration
public class TestApplicationConfig extends AbstractCouchbaseConfiguration {

  @Autowired
  private Environment env;

  @Bean
  public String couchbaseAdminUser() {
    return env.getProperty("couchbase.adminUser", "Administrator");
  }

  @Bean
  public String couchbaseAdminPassword() {
    return env.getProperty("couchbase.adminUser", "password");
  }

  @Override
  protected List<String> bootstrapHosts() {
    return Arrays.asList(env.getProperty("couchbase.host", "127.0.0.1"));
  }

  @Override
  protected String getBucketName() {
    return env.getProperty("couchbase.bucket", "default");
  }

  @Override
  protected String getBucketPassword() {
    return env.getProperty("couchbase.password", "");
  }

  @Bean
  public BucketCreator bucketCreator() throws Exception {
    return new BucketCreator(bootstrapHosts().get(0), couchbaseAdminUser(), couchbaseAdminPassword());
  }

  @Bean
  @Override
  @DependsOn("bucketCreator")
  public CouchbaseClient couchbaseClient() throws Exception {
    setLoggerProperty(couchbaseLogger());

    CouchbaseConnectionFactoryBuilder builder = new CouchbaseConnectionFactoryBuilder();
    builder.setOpTimeout(10000); // using a higher timeout for tests to reduce flakiness

    return new CouchbaseClient(builder.buildCouchbaseConnection(
      bootstrapUris(bootstrapHosts()),
      getBucketName(),
      getBucketPassword()
    ));
  }

  @Override
  public CouchbaseTemplate couchbaseTemplate() throws Exception {
    CouchbaseTemplate template = super.couchbaseTemplate();
    template.setWriteResultChecking(WriteResultChecking.LOG);
    return template;
  }
}
