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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.util.BucketManager;

/**
 * @author Michael Nitschinger
 */
@Configuration
public class TestApplicationConfig  {

  @Autowired
  private Environment env;

  @Bean
  public String couchbaseHost() {
    return env.getProperty("couchbase.host", "http://127.0.0.1:8091/pools");
  }

  @Bean
  public String couchbaseBucket() {
    return env.getProperty("couchbase.bucket", "default");
  }

  @Bean
  public String couchbasePassword() {
    return env.getProperty("couchbase.password", "");
  }

  @Bean
  public String couchbaseAdmin() {
    return env.getProperty("couchbase.admin", "Administrator");
  }

  @Bean
  public String couchbaseAdminPassword() {
    return env.getProperty("couchbase.adminPassword", "password");
  }

  @Bean
  public BucketManager bucketManager() {
    return new BucketManager(couchbaseHost(), couchbaseAdmin(), couchbaseAdminPassword());
  }

  @Bean
  public MappingCouchbaseConverter mappingCouchbaseConverter() throws Exception {
    return new MappingCouchbaseConverter(couchbaseMappingContext());
  }

  @Bean
  public CouchbaseMappingContext couchbaseMappingContext() throws Exception {
    return new CouchbaseMappingContext();
  }

}
