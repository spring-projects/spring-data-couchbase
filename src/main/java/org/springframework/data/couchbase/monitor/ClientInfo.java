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

import java.util.List;

import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.java.Collection;

import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

/**
 * Exposes basic client information.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 */
@ManagedResource(description = "Client Information")
public class ClientInfo {

  private final Collection collection;

  public ClientInfo(final Collection collection) {
    this.collection = collection;
  }

  @ManagedAttribute(description = "Hostnames of connected nodes")
  public String getHostNames() {
    List<NodeInfo> nodes = collection.core().clusterConfig().bucketConfig(collection.bucketName()).nodes();
    StringBuilder result = new StringBuilder();
    for (NodeInfo node : nodes) {
      result.append(node.toString()).append(",");
    }
    return result.toString();
  }

  @ManagedAttribute(description = "Number of connected nodes")
  public int getNumberOfNodes() {
    return collection.core().clusterConfig().bucketConfig(collection.bucketName()).nodes().size();
  }

  //TODO obtain count of available nodes vs unavailable ones and expose it

}
