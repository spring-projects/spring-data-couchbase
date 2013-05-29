package com.couchbase.spring.monitor;

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
    List<SocketAddress> available = (ArrayList) client.getAvailableServers();
    Collections.shuffle(available);
    return ((InetSocketAddress) available.get(0)).getHostName();
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
