package org.springframework.data.couchbase.config;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.env.CouchbaseEnvironment;

/**
 * Strategy interface for users to provide as a factory for custom components needed
 * by the Couchbase integration.
 *
 * @author Stephane Nicoll
 */
public interface CouchbaseConfigurer {

	CouchbaseEnvironment couchbaseEnvironment() throws Exception;

	Cluster couchbaseCluster() throws Exception;

	ClusterInfo couchbaseClusterInfo() throws Exception;

	Bucket couchbaseClient() throws Exception;

}
