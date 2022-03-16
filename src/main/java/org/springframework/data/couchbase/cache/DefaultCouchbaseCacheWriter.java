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

import static com.couchbase.client.java.kv.GetOptions.*;
import static com.couchbase.client.java.kv.InsertOptions.*;
import static com.couchbase.client.java.kv.UpsertOptions.*;
import static com.couchbase.client.java.query.QueryOptions.*;
import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;

import java.time.Duration;

import org.springframework.data.couchbase.CouchbaseClientFactory;

import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.query.QueryMetrics;
import com.couchbase.client.java.query.QueryResult;

public class DefaultCouchbaseCacheWriter implements CouchbaseCacheWriter {

	private final CouchbaseClientFactory clientFactory;

	public DefaultCouchbaseCacheWriter(final CouchbaseClientFactory clientFactory) {
		this.clientFactory = clientFactory;
	}

	@Override
	public void put(final String collectionName, final String key, final Object value, final Duration expiry,
			final Transcoder transcoder) {
		UpsertOptions options = upsertOptions();

		if (expiry != null) {
			options.expiry(expiry);
		}
		if (transcoder != null) {
			options.transcoder(transcoder);
		}

		getCollection(collectionName).upsert(key, value, options);
	}

	@Override
	public Object putIfAbsent(final String collectionName, final String key, final Object value, final Duration expiry,
			final Transcoder transcoder) {
		InsertOptions options = insertOptions();

		if (expiry != null) {
			options.expiry(expiry);
		}
		if (transcoder != null) {
			options.transcoder(transcoder);
		}

		try {
			getCollection(collectionName).insert(key, value, options);
			return null;
		} catch (final DocumentExistsException ex) {
			// If the document exists, return the current one per contract
			return get(collectionName, key, transcoder);
		}
	}

	@Override
	public Object get(final String collectionName, final String key, final Transcoder transcoder) {
		// TODO .. the decoding side transcoding needs to be figured out?
		try {
			return getCollection(collectionName).get(key, getOptions().transcoder(transcoder)).contentAs(Object.class);
		} catch (DocumentNotFoundException ex) {
			return null;
		}
	}

	@Override
	public boolean remove(final String collectionName, final String key) {
		try {
			getCollection(collectionName).remove(key);
			return true;
		} catch (final DocumentNotFoundException ex) {
			return false;
		}
	}

	@Override
	public long clear(final String collectionName, final String pattern) {
		QueryResult result = clientFactory.getScope().query(
				"DELETE FROM `" + collectionName + "` where meta().id LIKE $pattern",
				queryOptions().scanConsistency(REQUEST_PLUS).metrics(true).parameters(JsonObject.create().put("pattern", pattern + "%")));
		return result.metaData().metrics().map(QueryMetrics::mutationCount).orElse(0L);
	}

	private Collection getCollection(final String collectionName) {
		final Scope scope = clientFactory.getScope();
		if (collectionName == null) {
			if (!scope.name().equals(CollectionIdentifier.DEFAULT_SCOPE)) {
				throw new IllegalStateException("A collectionName must be provided if a non-default scope is used!");
			}
			return clientFactory.getBucket().defaultCollection();
		}
		return scope.collection(collectionName);
	}
}
