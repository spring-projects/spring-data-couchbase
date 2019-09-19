/*
 * Copyright 2012-2019 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.monitor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.java.Bucket;

import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.web.client.RestTemplate;

/**
 * Exposes basic cluster information.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 */
@ManagedResource(description = "Cluster Information")
public class ClusterInfo {

  private final RestTemplate template;
  private final Bucket bucket;
  private final BucketConfig info;

  public ClusterInfo(final Bucket bucket) {
    this.template = new RestTemplate();
    this.bucket = bucket;
    this.info = bucket.core().clusterConfig().bucketConfig(bucket.name());
  }

  @ManagedMetric(description = "Total RAM assigned")
  public long getTotalRAMAssigned() {
    return convertPotentialLong(parseStorageTotals().get("ram").get("total"));
  }

  @ManagedMetric(description = "Total RAM used")
  public long getTotalRAMUsed() {
    return convertPotentialLong(parseStorageTotals().get("ram").get("used"));
  }

  @ManagedMetric(description = "Total Disk Space assigned")
  public long getTotalDiskAssigned() {
    return convertPotentialLong(parseStorageTotals().get("hdd").get("total"));
  }

  @ManagedMetric(description = "Total Disk Space used")
  public long getTotalDiskUsed() {
    return convertPotentialLong(parseStorageTotals().get("hdd").get("used"));
  }

  @ManagedMetric(description = "Total Disk Space free")
  public long getTotalDiskFree() {
    return convertPotentialLong(parseStorageTotals().get("hdd").get("free"));
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

  /**
   * Depending on the value size, either int or long can be passed in and get
   * converted to long.
   *
   * @param value the value to convert.
   * @return the converted value.
   */
  private long convertPotentialLong(Object value) {
    if (value instanceof Integer) {
      return new Long((Integer) value);
    }
    else if (value instanceof Long) {
      return (Long) value;
    }
    else {
      throw new IllegalStateException("Cannot convert value to long: " + value);
    }
  }

  protected String randomAvailableHostname() {
    List<NodeInfo> available = info.nodes();
    Collections.shuffle(available);
    return available.get(0).hostname();
  }

  private HashMap<String, Object> fetchPoolInfo() {
    return template.getForObject("http://"
        + randomAvailableHostname() + ":8091/pools/default", HashMap.class);
  }

  private HashMap<String, HashMap> parseStorageTotals() {
    HashMap<String, Object> stats = fetchPoolInfo();
    return (HashMap<String, HashMap>) stats.get("storageTotals");
  }

}
