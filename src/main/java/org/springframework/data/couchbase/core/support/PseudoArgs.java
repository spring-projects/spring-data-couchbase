/*
 * Copyright 2021-2022 the original author or authors
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

import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.io.CollectionIdentifier;

/**
 * Determine the arguments to be used in the operation from various sources
 *
 * @author Michael Reiche
 * @param <OPTS>
 */
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

		// threadlocal comes from the scope/collection of a repository from DynamicProxy via template threadLocal
		// it - has precedence over the annotation of the method/entityClass/repositoryClass in the scope/collection args.
		// note that there is no withScope()/withCollection() for repositories, so the scope/collection args can
		// only be from annotations when scopeForQuery/collectionForQuery are non-null.
		//
		// for templates, there is no threadLocal, therefore scopeForQuery/collectionForQuery are always null

		PseudoArgs<OPTS> threadLocal = (PseudoArgs<OPTS>) template.getPseudoArgs();
		template.setPseudoArgs(null);
		if (threadLocal != null) {
			scopeForQuery = threadLocal.getScope();
			collectionForQuery = threadLocal.getCollection();
			optionsForQuery = threadLocal.getOptions();
		}

		// the scope and collection args can come from
		// - an annotation on the entity class in creation of the operation
		// i.e. new ExecutableFindByIdSupport<>(template, domainType, OptionsBuilder.getScopeFrom(domainType),
		// OptionsBuilder.getCollectionFrom(domainType)...
		//
		// - from CouchbaseRepositoryBase.getScope() that checks
		// 1) crudMethodMetadata
		// 2) entityClass annotation
		// 3) repositoryClass annotation
		// Note that it does not have the method to check for annotations. Only methods implemented in the base class
		// are processed through the CouchbaseRepository class.
		//
		// - from the constructor of AbstractCouchbaseQueryBase.
		// findOp = (ExecutableFindByQuery<?>) (findOp.inScope(method.getScope()).inCollection(method.getCollection()));
		// so is it also needed in the execute???

		scopeForQuery = fromFirst(null, scopeForQuery, scope);
		collectionForQuery = fromFirst(null, collectionForQuery, collection);
		optionsForQuery = fromFirst(null, options, optionsForQuery);

		// if a collection was specified but no scope, use the scope from the clientFactory

		if (collectionForQuery != null && scopeForQuery == null) {
			scopeForQuery = template.getScopeName();
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
		if (scopeForQuery != null && collectionForQuery == null) {
			throw new CouchbaseException(
					new IllegalArgumentException("if scope is not default or null, then collection must be specified"));
		}
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
