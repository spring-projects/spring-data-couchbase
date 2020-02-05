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

package org.springframework.data.couchbase.cache;

import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.query.QueryMetrics;
import com.couchbase.client.java.query.QueryResult;
import org.springframework.data.couchbase.connection.CouchbaseConnectionFactory;

import java.time.Duration;

import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;

public class DefaultCouchbaseCacheWriter implements CouchbaseCacheWriter {

  private final CouchbaseConnectionFactory connectionFactory;

  public DefaultCouchbaseCacheWriter(final CouchbaseConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
  }

  @Override
  public void put(final String bucketName, final String scopeName, final String collectionName,
                  final String key, final Object value, final Duration expiry,
                  final Transcoder transcoder) {
    UpsertOptions options = upsertOptions();

    if (expiry != null) {
      options.expiry(expiry);
    }
    if (transcoder != null) {
      options.transcoder(transcoder);
    }

    connectionFactory.getCollection(bucketName, scopeName, collectionName).upsert(key, value, options);
  }

  @Override
  public Object putIfAbsent(final String bucketName, final String scopeName, final String collectionName,
                            final String key, final Object value, final Duration expiry,
                          final Transcoder transcoder) {
    InsertOptions options = insertOptions();

    if (expiry != null) {
      options.expiry(expiry);
    }
    if (transcoder != null) {
      options.transcoder(transcoder);
    }

    try {
      connectionFactory.getCollection(bucketName, scopeName, collectionName).insert(key, value, options);
      return null;
    } catch (final DocumentExistsException ex) {
      // If the document exists, return the current one per contract
      return get(bucketName, scopeName, collectionName, key, transcoder);
    }
  }

  @Override
  public Object get(final String bucketName, final String scopeName, final String collectionName,
                    final String key, final Transcoder transcoder) {
    // TODO .. the decoding side transcoding needs to be figured out?
    try {
      return connectionFactory
        .getCollection(bucketName, scopeName, collectionName)
        .get(key, getOptions().transcoder(transcoder))
        .contentAs(Object.class);
    } catch (DocumentNotFoundException ex) {
      return null;
    }
  }

  @Override
  public boolean remove(final String bucketName, final String scopeName, final String collectionName, final String key) {
    try {
      connectionFactory.getCollection(bucketName, scopeName, collectionName).remove(key);
      return true;
    } catch (final DocumentNotFoundException ex) {
      return false;
    }
  }

  @Override
  public long clear(final String bucketName, final String pattern) {
    QueryResult result = connectionFactory.getCluster().query(
      "DELETE FROM `" + bucketName + "` where meta().id LIKE $pattern",
      queryOptions().metrics(true).parameters(JsonObject.create().put("pattern", pattern + "%"))
    );
    return result.metaData().metrics().map(QueryMetrics::mutationCount).orElse(0L);
  }
}
