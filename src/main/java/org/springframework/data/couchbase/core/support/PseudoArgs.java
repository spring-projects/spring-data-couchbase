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

import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.mapping.Document;

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
	 * @param template to hold
	 * @param scope
	 * @param collection
	 * @param options
	 * @param domainType
	 */
	public PseudoArgs(ReactiveCouchbaseTemplate template, String scope, String collection, OPTS options,
			Class<?> domainType) {

		// ??? 1) template - values from the args (fluent api) [ or annotations??? ]

		String scopeForQuery = null; // = scope;
		String collectionForQuery = null; // = collection;
		OPTS optionsForQuery = null; // = options;

		// 2) repository from DynamicProxy via template threadLocal - has precedence over annotation

		PseudoArgs<OPTS> threadLocal = (PseudoArgs<OPTS>) template.getPseudoArgs();
		template.setPseudoArgs(null);
		if (threadLocal != null) { // repository.withScope()
			scopeForQuery = /* scopeForQuery != null ? scopeForQuery : */ threadLocal.getScope();
			collectionForQuery = /* collectionForQuery != null ? collectionForQuery : */ threadLocal.getCollection();
			optionsForQuery = /* optionsForQuery != null ? optionsForQuery : */ threadLocal.getOptions();
		}

		if (scopeForQuery == null) {
			if (scope != null /* && !CollectionIdentifier.DEFAULT_SCOPE.equals(scope) */) { // from withScope(scope)
				scopeForQuery = scope;
			}
		}
		if (collectionForQuery == null) {
			if (collection != null /* && !CollectionIdentifier.DEFAULT_COLLECTION.equals(collection) */) { // withCollection(collection)
				collectionForQuery = collection;
			}
		}
		if (optionsForQuery == null) {
			if (options != null) {
				optionsForQuery = options;
			}
		}

		if (scopeForQuery == null) { // from entity class
			scopeForQuery = getScopeAnnotation(domainType);
		}

		if (collectionForQuery == null) {
			collectionForQuery = getCollectionAnnotation(domainType);
		}
		// if a collection was specified but no scope, use the scope from the clientFactory

		if (collectionForQuery != null && scopeForQuery == null) {
			scopeForQuery = template.getCouchbaseClientFactory().getScope().name();
		}

		// specifying scope and collection = _default is not necessary and will fail if server doesn't have collections

		if (scopeForQuery == null || CollectionIdentifier.DEFAULT_SCOPE.equals(scopeForQuery)) {
			if (collectionForQuery == null || CollectionIdentifier.DEFAULT_COLLECTION.equals(collectionForQuery)) {
				collectionForQuery = null;
			}
			if (collectionForQuery == null || !CollectionIdentifier.DEFAULT_COLLECTION.equals(collectionForQuery)) { // if
																																																								// collection
																																																								// isn't
																																																								// null,
																																																								// then
																																																								// (maybe)
																																																								// use
																																																								// template.getScope(),
																																																								// otherwise
																																																								// null
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

	public String getScopeAnnotation(Class<?> domainType) {
		// Document d = AnnotatedElementUtils.findMergedAnnotation(entityInformation.getJavaType(), Document.class);
		if (domainType == null) {
			return null;
		}
		Document documentAnnotation = domainType.getAnnotation(Document.class);
		if (documentAnnotation != null && documentAnnotation.scope() != null
				&& !CollectionIdentifier.DEFAULT_SCOPE.equals(documentAnnotation.scope())) {
			return documentAnnotation.scope();
		}
		return null;
	}

	public String getCollectionAnnotation(Class<?> domainType) {
		if (domainType == null) {
			return null;
		}
		Document documentAnnotation = domainType.getAnnotation(Document.class);
		if (documentAnnotation != null && documentAnnotation.collection() != null
				&& !CollectionIdentifier.DEFAULT_COLLECTION.equals(documentAnnotation.collection())) {
			return documentAnnotation.collection();
		}
		return null;
	}

	@Override
	public String toString() {
		return "scope: " + getScope() + " collection: " + getCollection() + " options: " + getOptions();
	}
}
