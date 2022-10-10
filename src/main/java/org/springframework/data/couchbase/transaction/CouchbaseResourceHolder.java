/*
 * Copyright 2022 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

import org.springframework.lang.Nullable;
import org.springframework.transaction.support.ResourceHolderSupport;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;

/**
 * Container for couchbase transaction resources to hold in threadlocal or reactive context.
 *
 * @author Michael Reiche
 *
 */
@Stability.Internal
public class CouchbaseResourceHolder extends ResourceHolderSupport {

	private @Nullable CoreTransactionAttemptContext core; // which holds the atr
	Map<Integer, Object> getResultMap = new HashMap<>();

	/**
	 * Create a new {@link CouchbaseResourceHolder} for a given {@link CoreTransactionAttemptContext session}.
	 *
	 * @param core the associated {@link CoreTransactionAttemptContext}. Can be {@literal null}.
	 */
	public CouchbaseResourceHolder(@Nullable CoreTransactionAttemptContext core) {
		this.core = core;
	}

	/**
	 * @return the associated {@link CoreTransactionAttemptContext}. Can be {@literal null}.
	 */
	@Nullable
	public CoreTransactionAttemptContext getCore() {
		return core;
	}

	public Object transactionResultHolder(Object holder, Object o) {
		getResultMap.put(System.identityHashCode(o), holder);
		return holder;
	}

}
