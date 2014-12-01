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

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.WriteResultChecking;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;

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
  public Bucket couchbaseClient(final CouchbaseCluster cluster) throws Exception {
    return super.couchbaseClient(cluster);
  }

  @Override
  public CouchbaseTemplate couchbaseTemplate(final Bucket bucket) throws Exception {
    CouchbaseTemplate template = super.couchbaseTemplate(bucket);
    template.setWriteResultChecking(WriteResultChecking.LOG);
    return template;
  }
}
