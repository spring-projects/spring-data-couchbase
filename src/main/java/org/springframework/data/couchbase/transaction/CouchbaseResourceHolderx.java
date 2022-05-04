/*
   * Copyright 2019-2021 the original author or authors.
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

import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.lang.Nullable;
import org.springframework.transaction.support.ResourceHolderSupport;

import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;

/**
 * MongoDB specific resource holder, wrapping a {@link CoreTransactionAttemptContext}.
 * {@link ReactiveCouchbaseTransactionManager} binds instances of this class to the subscriber context.
 * <p />
 * <strong>Note:</strong> Intended for internal usage only.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 * @see CouchbaseTransactionManager
 * @see CouchbaseTemplate
 */
// todo gp understand why this is needed - can we not just hold ctx in Mono context?
public class CouchbaseResourceHolderx extends ResourceHolderSupport {

	private @Nullable CoreTransactionAttemptContext core; // which holds the atr
	private CouchbaseClientFactory databaseFactory;

	/**
	 * Create a new {@link org.springframework.data.couchbase.transaction.ReactiveCouchbaseResourceHolder} for a given
	 * {@link CoreTransactionAttemptContext session}.
	 *
	 * @param core the associated {@link CoreTransactionAttemptContext}. Can be {@literal null}.
	 * @param databaseFactory the associated {@link CouchbaseClientFactory}. must not be {@literal null}.
	 */
	public CouchbaseResourceHolderx(@Nullable CoreTransactionAttemptContext core, CouchbaseClientFactory databaseFactory) {

		this.core = core;
		this.databaseFactory = databaseFactory;
	}

	/**
	 * @return the associated {@link CoreTransactionAttemptContext}. Can be {@literal null}.
	 */
	@Nullable
	public CoreTransactionAttemptContext getCore() {
		return core;
	}

	/**
	 * @return the required associated {@link CoreTransactionAttemptContext}.
	 * @throws IllegalStateException if no session is associated.
	 */
	CoreTransactionAttemptContext getRequiredSession() {

		CoreTransactionAttemptContext session = getCore();

		if (session == null) {
			throw new IllegalStateException("No ClientSession associated");
		}
		return session;
	}

	/**
	 * @return the associated {@link CouchbaseClientFactory}.
	 */
	public CouchbaseClientFactory getDatabaseFactory() {
		return databaseFactory;
	}

	/**
	 * Set the {@link CoreTransactionAttemptContext} to guard.
	 *
	 * @param core can be {@literal null}.
	 */
	public void setCore(@Nullable CoreTransactionAttemptContext core) {
		this.core = core;
	}

	/**
	 * @return {@literal true} if session is not {@literal null}.
	 */
	boolean hasCore() {
		return core != null;
	}

	/**
	 * If the {@link org.springframework.data.couchbase.transaction.ReactiveCouchbaseResourceHolder} is {@link #hasCore()}
	 * not already associated} with a {@link CoreTransactionAttemptContext} the given value is
	 * {@link #setCore(CoreTransactionAttemptContext) set} and returned, otherwise the current bound session is returned.
	 *
	 * @param core
	 * @return
	 */
	@Nullable
	public CoreTransactionAttemptContext setSessionIfAbsent(@Nullable CoreTransactionAttemptContext core) {

		if (!hasCore()) {
			setCore(core);
		}

		return core;
	}

}
