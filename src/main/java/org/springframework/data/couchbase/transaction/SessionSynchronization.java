/*
 * Copyright 2018-2021 the original author or authors.
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
package org.springframework.data.couchbase.transaction;

/**
 * {@link SessionSynchronization} is used along with {@link org.springframework.data.couchbase.core.CouchbaseTemplate} to
 * define in which type of transactions to participate if any.
 *
 * @author Michael Reiche
 */
public enum SessionSynchronization {

	/**
	 * Synchronize with any transaction even with empty transactions and initiate a MongoDB transaction when doing so by
	 * registering a MongoDB specific {@link org.springframework.transaction.support.ResourceHolderSynchronization}.
	 */
	ALWAYS,

	/**
	 * Synchronize with native couchbase transactions initiated via {@link ReactiveCouchbaseTransactionManager}.
	 */
	ON_ACTUAL_TRANSACTION,
	NEVER;

}
