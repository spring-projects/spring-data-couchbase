/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.util;

import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * This configuration is populated from the cluster container and represents the
 * settings that can be used from the tests for bootstrapping their own code.
 *
 * @since 2.0.0
 */
public class TestClusterConfig {

  private final String bucketname;
  private final String adminUsername;
  private final String adminPassword;
  private final List<TestNodeConfig> nodes;
  private final int numReplicas;
  private final Optional<X509Certificate> clusterCert;
  private final Set<Capabilities> capabilities;

  TestClusterConfig(String bucketname, String adminUsername, String adminPassword,
                    List<TestNodeConfig> nodes, int numReplicas,
                    Optional<X509Certificate> clusterCert, Set<Capabilities> capabilities) {
    this.bucketname = bucketname;
    this.adminUsername = adminUsername;
    this.adminPassword = adminPassword;
    this.nodes = nodes;
    this.numReplicas = numReplicas;
    this.clusterCert = clusterCert;
    this.capabilities = capabilities;
  }

  public String bucketname() {
    return bucketname;
  }

  public String adminUsername() {
    return adminUsername;
  }

  public String adminPassword() {
    return adminPassword;
  }

  public List<TestNodeConfig> nodes() {
    return nodes;
  }

  public int numReplicas() {
    return numReplicas;
  }

  public Set<Capabilities> capabilities() {
    return capabilities;
  }

  public Optional<X509Certificate> clusterCert() {
    return clusterCert;
  }

  /**
   * Finds the first node with a given service enabled in the config.
   *
   * <p>This method can be used to find bootstrap nodes and similar.</p>
   *
   * @param service the service to find.
   * @return a node config if found, empty otherwise.
   */
  public Optional<TestNodeConfig> firstNodeWith(Services service) {
    return nodes.stream().filter(n -> n.ports().containsKey(service)).findFirst();
  }

  @Override
  public String toString() {
    return "TestClusterConfig{" +
      "bucketname='" + bucketname + '\'' +
      ", adminUsername='" + adminUsername + '\'' +
      ", adminPassword='" + adminPassword + '\'' +
      ", nodes=" + nodes +
      ", numReplicas=" + numReplicas +
      ", clusterCert=" + clusterCert +
      ", capabilities=" + capabilities +
      '}';
  }
}
