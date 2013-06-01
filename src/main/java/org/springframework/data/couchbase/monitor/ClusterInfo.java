package org.springframework.data.couchbase.monitor;

import com.couchbase.client.CouchbaseClient;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJacksonHttpMessageConverter;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.HashMap;

/**
 * Exposes basic cluster information.
 */
@ManagedResource(description =  "Cluster Information")
public class ClusterInfo extends AbstractMonitor {

  public ClusterInfo(final CouchbaseClient client) {
    super(client);
  }

  @ManagedMetric(description = "Total RAM assigned")
  public long getTotalRAMAssigned() {
    return (Long) parseStorageTotals().get("ram").get("total");
  }

  @ManagedMetric(description = "Total RAM used")
  public long getTotalRAMUsed() {
    return (Long) parseStorageTotals().get("ram").get("used");
  }

  @ManagedMetric(description = "Total Disk Space assigned")
  public long getTotalDiskAssigned() {
    return (Long) parseStorageTotals().get("hdd").get("total");
  }

  @ManagedMetric(description = "Total Disk Space used")
  public long getTotalDiskUsed() {
    return (Long) parseStorageTotals().get("hdd").get("used");
  }

  @ManagedMetric(description = "Total Disk Space free")
  public long getTotalDiskFree() {
    return (Long) parseStorageTotals().get("hdd").get("free");
  }

  @ManagedAttribute(description = "Cluster is Balanced")
  public boolean getIsBalanced() {
    return (Boolean) fetchPoolInfo().get("balanced");
  }

  @ManagedAttribute(description = "Rebalance Status")
  public String getRebalanceStatus() {
    return (String) fetchPoolInfo().get("rebalanceStatus");
  }

  @ManagedAttribute(description = "Maximum Available Buckets")
  public int getMaxBuckets() {
    return (Integer) fetchPoolInfo().get("maxBucketCount");
  }

  private HashMap<String, Object> fetchPoolInfo() {
    return getTemplate().getForObject("http://"
      + randomAvailableHostname() + ":8091/pools/default", HashMap.class);
  }

  private HashMap<String, HashMap> parseStorageTotals() {
    HashMap<String, Object> stats = fetchPoolInfo();
    return  (HashMap<String, HashMap>) stats.get("storageTotals");
  }

}
