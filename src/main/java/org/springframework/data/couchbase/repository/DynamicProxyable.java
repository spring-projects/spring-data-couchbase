/*
 * Copyright 2017-2021 the original author or authors.
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

package org.springframework.data.couchbase.repository;

import java.lang.reflect.Proxy;

import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.support.DynamicInvocationHandler;

import com.couchbase.client.java.CommonOptions;

/**
 * The generic parameter needs to be REPO which is either a CouchbaseRepository parameterized on T,ID or a
 * ReactiveCouchbaseRepository parameterized on T,ID. i.e.: interface AirportRepository extends
 * CouchbaseRepository&lt;Airport, String&gt;, DynamicProxyable&lt;AirportRepository&gt;
 * 
 * @param <REPO>
 * @author Michael Reiche
 */
public interface DynamicProxyable<REPO> {

	CouchbaseEntityInformation getEntityInformation();

	Object getOperations();

	/**
	 * Support for Couchbase-specific options, scope and collections The three "with" methods will return a new proxy
	 * instance with the specified options, scope, or collections set. The setters are called with the corresponding
	 * options, scope and collection to set the ThreadLocal fields on the CouchbaseOperations of the repository just
	 * before the call is made to the repository, and called again with 'null' just after the call is made. The repository
	 * method will fetch those values to use in the call.
	 */

	/**
	 * @param options - the options to set on the returned repository object
	 */
	@SuppressWarnings("unchecked")
	default REPO withOptions(CommonOptions<?> options) {
		REPO proxyInstance = (REPO) Proxy.newProxyInstance(this.getClass().getClassLoader(),
				this.getClass().getInterfaces(), new DynamicInvocationHandler(this, options, null, (String) null));
		return proxyInstance;
	}

	/**
	 * @param scope - the scope to set on the returned repository object
	 */
	@SuppressWarnings("unchecked")
	default REPO withScope(String scope) {
		REPO proxyInstance = (REPO) Proxy.newProxyInstance(this.getClass().getClassLoader(),
				this.getClass().getInterfaces(), new DynamicInvocationHandler<>(this, null, null, scope));
		return proxyInstance;
	}

	/**
	 * @param collection - the collection to set on the returned repository object
	 */
	@SuppressWarnings("unchecked")
	default REPO withCollection(String collection) {
		REPO proxyInstance = (REPO) Proxy.newProxyInstance(this.getClass().getClassLoader(),
				this.getClass().getInterfaces(), new DynamicInvocationHandler<>(this, null, collection, null));
		return proxyInstance;
	}

}
