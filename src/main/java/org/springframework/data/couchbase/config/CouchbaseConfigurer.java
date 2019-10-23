package org.springframework.data.couchbase.config;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;

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
   * Set up the underlying main {@link ClusterEnvironment}, allowing tuning of the Couchbase SDK.
   *
   * @throws Exception in case of error during the CouchbaseEnvironment instantiation.
   */
  //ClusterEnvironment couchbaseEnvironment() throws Exception;

  /**
   * Set up the underlying main Couchbase {@link Cluster} reference to be used by the Spring Data framework
   * when storing into Couchbase.
   *
   * @throws Exception in case of error during the Cluster instantiation.
   */
  Cluster couchbaseCluster() throws Exception;

  /**
   * Set up the underlying main {@link Collection}, the primary Couchbase SDK entry point to be used by the Spring Data
   * framework for the {@link CouchbaseOperations}.
   *
   * @throws Exception in case of error during the bucket instantiation.
   */
  Collection couchbaseClient() throws Exception;

  Bucket couchbaseBucket() throws Exception;

}
