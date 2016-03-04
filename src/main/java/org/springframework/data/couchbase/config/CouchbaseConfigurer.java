package org.springframework.data.couchbase.config;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.env.CouchbaseEnvironment;

import org.springframework.data.couchbase.core.CouchbaseOperations;

/**
 * Strategy interface for users to provide as a factory for custom components needed
 * by the Couchbase integration.
 *
 * This allows to centralize instantiation of Couchbase SDK core elements.
 *
 * @author Stephane Nicoll
 */
public interface CouchbaseConfigurer {

  /**
   * Set up the underlying main {@link CouchbaseEnvironment}, allowing tuning of the Couchbase SDK.
   *
   * @throws Exception in case of error during the CouchbaseEnvironment instantiation.
   */
  CouchbaseEnvironment couchbaseEnvironment() throws Exception;

  /**
   * Set up the underlying main Couchbase {@link Cluster} reference to be used by the Spring Data framework
   * when storing into Couchbase.
   *
   * @throws Exception in case of error during the Cluster instantiation.
   */
  Cluster couchbaseCluster() throws Exception;

  /**
   * Set up the underlying main {@link ClusterInfo}, allowing to check feature availability and cluster configuration.
   *
   * @throws Exception in case of error during the ClusterInfo instantiation.
   */
  ClusterInfo couchbaseClusterInfo() throws Exception;

  /**
   * Set up the underlying main {@link Bucket}, the primary Couchbase SDK entry point to be used by the Spring Data
   * framework for the {@link CouchbaseOperations}.
   *
   * @throws Exception in case of error during the bucket instantiation.
   */
  Bucket couchbaseClient() throws Exception;

}
