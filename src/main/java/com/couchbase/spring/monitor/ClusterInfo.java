package com.couchbase.spring.monitor;

import com.couchbase.client.CouchbaseClient;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import java.net.SocketAddress;

/**
 * Exposes basic cluster information.
 */
@ManagedResource(description =  "Cluster Information")
public class ClusterInfo extends AbstractMonitor {

  public ClusterInfo(final CouchbaseClient client) {
    super(client);
  }

  @ManagedOperation(description = "Cluster Hostnames")
  public String getHostNames() {
    StringBuilder result = new StringBuilder();
    for (SocketAddress node : getStats().keySet()) {
      result.append(node.toString()).append(",");
    }
    return result.toString();
  }

  @ManagedOperation(description = "Number of Nodes")
  public int getNumberOfNodes() {
    return getStats().keySet().size();
  }

}
