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
package org.springframework.data.couchbase.monitor;

import com.couchbase.client.CouchbaseClient;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import java.net.SocketAddress;

/**
 * Exposes basic client information.
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
