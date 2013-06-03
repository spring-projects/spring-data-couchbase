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
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import java.net.SocketAddress;

/**
 * Exposes basic client information.
 *
 * @author Michael Nitschinger
 */
@ManagedResource(description =  "Client Information")
public class ClientInfo extends AbstractMonitor {

  public ClientInfo(final CouchbaseClient client) {
    super(client);
  }

  @ManagedAttribute(description = "Hostnames of connected nodes")
  public String getHostNames() {
    StringBuilder result = new StringBuilder();
    for (SocketAddress node : getStats().keySet()) {
      result.append(node.toString()).append(",");
    }
    return result.toString();
  }

  @ManagedAttribute(description = "Number of connected nodes")
  public int getNumberOfNodes() {
    return getStats().keySet().size();
  }

  @ManagedAttribute(description = "Number of connected active nodes")
  public int getNumberOfActiveNodes() {
    return getClient().getAvailableServers().size();
  }

  @ManagedAttribute(description = "Number of connected inactive nodes")
  public int getNumberOfInactiveNodes() {
    return getClient().getUnavailableServers().size();
  }

}
