/*
 * Copyright 2012-2022 the original author or authors
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

package org.springframework.data.couchbase.repository.support;

import reactor.util.annotation.Nullable;

import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import com.couchbase.client.java.query.QueryResult;

/**
 * Holds previously obtained Transaction*Result
 *
 * @author Michael Reiche
 */
public class TransactionResultHolder {

	private final @Nullable CoreTransactionGetResult getResult;
	// todo gp needed?
	private final @Nullable QueryResult singleQueryResult;

	public TransactionResultHolder(CoreTransactionGetResult getResult) {
		// we don't need the content and we don't have access to the transcoder an txnMeta (and we don't need them either).
		// todo gp will need to expose a copy ctor if a copy is needed
		this.getResult = getResult;
		// this.getResult = new TransactionGetResult(getResult.id(), null, getResult.cas(), getResult.collection(),
		// getResult.links(), getResult.status(), getResult.documentMetadata(), null, null);
		this.singleQueryResult = null;
	}

	public TransactionResultHolder(QueryResult singleQueryResult) {
		this.getResult = null;
		this.singleQueryResult = singleQueryResult;
	}

	public @Nullable CoreTransactionGetResult transactionGetResult() {
		return getResult;
	}

	public @Nullable QueryResult singleQueryResult() {
		return singleQueryResult;
	}
}
