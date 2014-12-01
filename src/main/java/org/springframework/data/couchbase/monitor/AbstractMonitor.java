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

package org.springframework.data.couchbase.monitor;

import org.springframework.web.client.RestTemplate;

import com.couchbase.client.java.bucket.BucketManager;

/**
 * Base class to encapsulate common configuration settings.
 *
 * @author Michael Nitschinger
 */
public abstract class AbstractMonitor {

  private final BucketManager client;

  private RestTemplate template = new RestTemplate();

  protected AbstractMonitor(final BucketManager client) {
    this.client = client;
  }

  public BucketManager getClient() {
    return client;
  }

  protected RestTemplate getTemplate() {
    return template;
  }

}
