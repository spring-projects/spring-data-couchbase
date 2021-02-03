/*
 * Copyright 2012-2021 the original author or authors
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

package org.springframework.data.couchbase.core;

import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.support.PseudoArgs;

/**
 * Defines common operations on the Couchbase data source, most commonly implemented by
 * {@link ReactiveCouchbaseTemplate}.
 */
public interface ReactiveCouchbaseOperations extends ReactiveFluentCouchbaseOperations {

	/**
	 * Returns the converter used for this template/operations.
	 */
	CouchbaseConverter getConverter();

	/**
	 * The name of the bucket used.
	 */
	String getBucketName();

	/**
	 * The name of the scope used, null if the default scope is used.
	 */
	String getScopeName();

	/**
	 * Returns the underlying client factory.
	 */
	CouchbaseClientFactory getCouchbaseClientFactory();

	/**
	 * @param pseudoArgs - the metaArgs to set on the ThreadLocal field of the CouchbaseOperations
	 */
	void setThreadLocalArgs(PseudoArgs<?> pseudoArgs);

	/**
	 * @@return the metaArgs from the ThreadLocal field of the CouchbaseOperations
	 */
	PseudoArgs<?> getThreadLocalArgs();

}
