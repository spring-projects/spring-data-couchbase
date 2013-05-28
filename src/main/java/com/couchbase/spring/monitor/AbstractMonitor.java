package com.couchbase.spring.monitor;

import com.couchbase.client.CouchbaseClient;

import java.net.SocketAddress;
import java.util.Map;

/**
 * Base class to encapsulate common configuration settings.
 */
public abstract class AbstractMonitor {

  private CouchbaseClient client;

  protected AbstractMonitor(final CouchbaseClient client) {
    this.client = client;
  }

  public CouchbaseClient getClient() {
    return client;
  }

  /**
   * Fetches stats for all nodes.
   *
   * @return stats for each node
   */
  protected Map<SocketAddress,Map<String,String>> getStats() {
    return client.getStats();
  }

  /**
   * Returns stats for an individual node.
   *
   * @param node
   * @return
   */
  protected Map<String, String> getStats(SocketAddress node) {
    return getStats().get(node);
  }

}
