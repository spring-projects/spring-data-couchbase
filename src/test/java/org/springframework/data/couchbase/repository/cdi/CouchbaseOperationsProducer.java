/*
 * Copyright 2014-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.repository.cdi;

import javax.enterprise.inject.Produces;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;

/**
 * Produces a {@link CouchbaseOperations} instance for test usage.
 *
 * @author Mark Paluch
 */
class CouchbaseOperationsProducer {

  @Produces
  public CouchbaseOperations createCouchbaseOperations(Cluster cluster, Collection collection) throws Exception {
	return new CouchbaseTemplate(cluster, collection);
  }
	
  @Produces
  @OtherQualifier
  @PersonDB
  public CouchbaseOperations createQualifiedCouchbaseOperations(Cluster cluster, Collection collection) throws Exception {
    return new CouchbaseTemplate(cluster, collection);
  }

}
