package com.couchbase.spring.monitor;

import com.couchbase.client.CouchbaseClient;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Exposes basic cluster information.
 */
@ManagedResource(description =  "Cluster Information")
public class ClusterInfo extends AbstractMonitor {

  public ClusterInfo(final CouchbaseClient client) {
    super(client);
  }

  @ManagedAttribute(description = "All hostnames of nodes in the cluster")
  public String getHostNames() {
    StringBuilder result = new StringBuilder();
    for (SocketAddress node : getStats().keySet()) {
      result.append(node.toString()).append(",");
    }
    return result.toString();
  }

  @ManagedAttribute(description = "Number of nodes currently in the cluster")
  public int getNumberOfNodes() {
    return getStats().keySet().size();
  }

  @ManagedAttribute(description = "Couchbase Server versions of nodes in the cluster")
  public String getNodeVersions() {
    StringBuilder result = new StringBuilder();
    for(Map.Entry<SocketAddress,Map<String,String>> entry : getStats().entrySet()) {
      for(Map.Entry<String, String> stat : entry.getValue().entrySet()) {
        if (stat.getKey().equals("ep_version")) {
          result.append(stat.getValue()).append(",");
        }
      }
    }
    return result.toString();
  }

}
