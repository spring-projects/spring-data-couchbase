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
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;

/**
 * @author Michael Nitschinger
 */
@Configuration
public class TestApplicationConfig extends AbstractCouchbaseConfiguration {

  @Autowired
  private Environment env;

  @Bean
  @Override
  public CouchbaseClient couchbaseClient() throws IOException {
    String defaultHost = "http://127.0.0.1:8091/pools";
    String host = env.getProperty("couchbase.host", defaultHost);

    String bucket = env.getProperty("couchbase.bucket", "default");
    String pass = env.getProperty("couchbase.password", "");
    return new CouchbaseClient(Arrays.asList(URI.create(host)), bucket, pass);
  }

}
