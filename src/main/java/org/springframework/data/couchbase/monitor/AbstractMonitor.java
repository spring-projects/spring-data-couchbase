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

import com.couchbase.client.CouchbaseClient;
import org.springframework.web.client.RestTemplate;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Base class to encapsulate common configuration settings.
 *
 * @author Michael Nitschinger
 */
public abstract class AbstractMonitor {

  private CouchbaseClient client;

  private RestTemplate template = new RestTemplate();

  protected AbstractMonitor(final CouchbaseClient client) {
    this.client = client;
  }

  public CouchbaseClient getClient() {
    return client;
  }

  protected RestTemplate getTemplate() {
    return template;
  }

  protected String randomAvailableHostname() {
    List<SocketAddress> available = (ArrayList<SocketAddress>) client.getAvailableServers();
    Collections.shuffle(available);
    return ((InetSocketAddress) available.get(0)).getHostName();
  }

  /**
   * Fetches stats for all nodes.
   *
   * @return stats for each node
   */
  protected Map<SocketAddress, Map<String, String>> getStats() {
    return client.getStats();
  }

  /**
   * Returns stats for an individual node.
   */
  protected Map<String, String> getStats(SocketAddress node) {
    return getStats().get(node);
  }

}
