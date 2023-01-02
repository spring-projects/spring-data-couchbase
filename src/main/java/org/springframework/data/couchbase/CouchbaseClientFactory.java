/*
 * Copyright 2012-2023 the original author or authors
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

package org.springframework.data.couchbase;

import java.io.Closeable;

import org.springframework.dao.support.PersistenceExceptionTranslator;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;

/**
 * The {@link CouchbaseClientFactory} is the main way to get access to the managed SDK instance and resources.
 * <p>
 * Please note that a single factory is always bound to a {@link Bucket}, so if you need to access more than one
 * you need to initialize one factory for each.
 */
public interface CouchbaseClientFactory extends Closeable {

	/**
	 * Provides access to the managed SDK {@link Cluster} reference.
	 */
	Cluster getCluster();

	/**
	 * Provides access to the managed SDK {@link Bucket} reference.
	 */
	Bucket getBucket();

	/**
	 * Provides access to the managed SDK {@link Scope} reference.
	 */
	Scope getScope();

	/**
	 * Provides access to a collection (identified by its name) in managed SDK {@link Scope} reference.
	 *
	 * @param name the name of the collection. If null is passed in, the default collection is assumed.
	 */
	Collection getCollection(String name);

	/**
	 * Provides access to the default collection.
	 */
	Collection getDefaultCollection();

	/**
	 * Returns a new {@link CouchbaseClientFactory} set to the scope given as an argument.
	 *
	 * @param scopeName the name of the scope to use for all collection access.
	 * @return a new client factory, bound to the other scope.
	 */
	CouchbaseClientFactory withScope(String scopeName);

	/**
	 * The exception translator used on the factory.
	 */
	PersistenceExceptionTranslator getExceptionTranslator();

}
