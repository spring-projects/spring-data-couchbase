/*
 * Copyright 2021 the original author or authors
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
package org.springframework.data.couchbase.core.support;

import static org.springframework.data.couchbase.core.query.OptionsBuilder.fromFirst;
import static org.springframework.data.couchbase.core.query.OptionsBuilder.getCollectionFrom;
import static org.springframework.data.couchbase.core.query.OptionsBuilder.getScopeFrom;

import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;

import com.couchbase.client.core.io.CollectionIdentifier;

public class PseudoArgs<OPTS> {
	private final OPTS options;
	private final String scopeName;
	private final String collectionName;

	public PseudoArgs(String scopeName, String collectionName, OPTS options) {
		this.options = options;
		this.scopeName = scopeName;
		this.collectionName = collectionName;
	}

	/**
	 * return scope, collection and options in following precedence <br>
	 * 1) values from fluent api<br>
	 * 2) values from dynamic proxy (via template threadLocal)<br>
	 * 3) the values from the couchbaseClientFactory<br>
	 * 
	 * @param template which holds ThreadLocal pseudo args
	 * @param scope - from calling operation
	 * @param collection - from calling operation
	 * @param options - from calling operation
	 * @param domainType - entity that may have annotations
	 */
	public PseudoArgs(ReactiveCouchbaseTemplate template, String scope, String collection, OPTS options,
			Class<?> domainType) {

		String scopeForQuery = null;
		String collectionForQuery = null;
		OPTS optionsForQuery = null;

		// 1) repository from DynamicProxy via template threadLocal - has precedence over annotation

		PseudoArgs<OPTS> threadLocal = (PseudoArgs<OPTS>) template.getPseudoArgs();
		template.setPseudoArgs(null);
		if (threadLocal != null) {
			scopeForQuery = threadLocal.getScope();
			collectionForQuery = threadLocal.getCollection();
			optionsForQuery = threadLocal.getOptions();
		}

		scopeForQuery = fromFirst(null, scopeForQuery, scope, getScopeFrom(domainType));
		collectionForQuery = fromFirst(null, collectionForQuery, collection, getCollectionFrom(domainType));
		optionsForQuery = fromFirst(null, options, optionsForQuery);

		// if a collection was specified but no scope, use the scope from the clientFactory

		if (collectionForQuery != null && scopeForQuery == null) {
			scopeForQuery = template.getCouchbaseClientFactory().getScope().name();
		}

		// specifying scope and collection = _default is not necessary and will fail if server doesn't have collections

		if (scopeForQuery == null || CollectionIdentifier.DEFAULT_SCOPE.equals(scopeForQuery)) {
			if (collectionForQuery == null || CollectionIdentifier.DEFAULT_COLLECTION.equals(collectionForQuery)) {
				collectionForQuery = null;
				scopeForQuery = null;
			}
		}

		this.scopeName = scopeForQuery;
		this.collectionName = collectionForQuery;
		this.options = optionsForQuery;

	}

	/**
	 * @return the options
	 */
	public OPTS getOptions() {
		return this.options;
	}

	/**
	 * @return the scope name
	 */
	public String getScope() {
		return this.scopeName;
	}

	/**
	 * @return the collection name
	 */
	public String getCollection() {
		return this.collectionName;
	}

	@Override
	public String toString() {
		return "scope: " + getScope() + " collection: " + getCollection() + " options: " + getOptions();
	}
}
