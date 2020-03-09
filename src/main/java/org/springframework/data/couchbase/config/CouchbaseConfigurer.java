/*
 * Copyright 2012-2020 the original author or authors
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

package org.springframework.data.couchbase.config;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.ClusterEnvironment;

/**
 * Strategy interface for users to provide as a factory for custom components needed
 * by the Couchbase integration.
 *
 * This allows to centralize instantiation of Couchbase SDK core elements.
 *
 * @author Stephane Nicoll
 * @author Michael Nitschinger
 */
public interface CouchbaseConfigurer {

	/**
	 * Set up the underlying main {@link ClusterEnvironment}, allowing tuning of the Couchbase SDK.
	 *
	 * @throws Exception in case of error during the ClusterEnvironment instantiation.
	 */
	ClusterEnvironment clusterEnvironment() throws Exception;

	/**
	 * Set up the underlying main {@link Cluster} reference to be used by the Spring Data framework
	 * when storing into Couchbase.
	 *
	 * @throws Exception in case of error during the Cluster instantiation.
	 */
	Cluster couchbaseCluster() throws Exception;

}
