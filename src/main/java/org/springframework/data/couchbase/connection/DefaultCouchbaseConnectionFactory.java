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

package org.springframework.data.couchbase.connection;

import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;

public class DefaultCouchbaseConnectionFactory implements CouchbaseConnectionFactory {

  private final Cluster cluster;

  public DefaultCouchbaseConnectionFactory(final String connectionString, final String username, final String password) {
    this(connectionString, PasswordAuthenticator.create(username, password));
  }

  public DefaultCouchbaseConnectionFactory(final String connectionString, final Authenticator authenticator) {
    this.cluster = Cluster.connect(connectionString, ClusterOptions.clusterOptions(authenticator));
  }

  @Override
  public Cluster getCluster() {
    return cluster;
  }

  @Override
  public Bucket getBucket(final String bucketName) {
    return cluster.bucket(bucketName);
  }

  @Override
  public Scope getScope(final String bucketName, final String scopeName) {
    Bucket bucket = getBucket(bucketName);
    return scopeName != null && !scopeName.isEmpty() ? bucket.scope(scopeName) : bucket.defaultScope();
  }

  @Override
  public Collection getCollection(final String bucketName, final String collectionName) {
    final Bucket bucket = getBucket(bucketName);
    return collectionName == null || collectionName.isEmpty() ? bucket.defaultCollection() : bucket.collection(collectionName);
  }

  @Override
  public Collection getCollection(final String bucketName, final String scopeName, final String collectionName) {
    final Bucket bucket = getBucket(bucketName);
    if (scopeName == null && collectionName == null) {
      return bucket.defaultCollection();
    }

    final Scope scope = scopeName == null || scopeName.isEmpty()
      ? bucket.defaultScope()
      : bucket.scope(scopeName);
    return collectionName == null || collectionName.isEmpty()
      ? scope.collection(CollectionIdentifier.DEFAULT_COLLECTION)
      : scope.collection(collectionName);
  }

}
