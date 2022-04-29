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

import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;

// todo gp experimenting with the simplest possible class, extending PlatformTransactionManager not AbstractPlatformTransactionManager
public class CouchbaseSimpleTransactionManager implements PlatformTransactionManager {

	private final CouchbaseClientFactory clientFactory;

	public CouchbaseSimpleTransactionManager(CouchbaseClientFactory clientFactory) {
		this.clientFactory = clientFactory;
	}

	@Override
	public TransactionStatus getTransaction(TransactionDefinition definition) throws TransactionException {
		return null;
	}

	@Override
	public void commit(TransactionStatus status) throws TransactionException {
		// todo gp what here - do we need to re-allow explicit commit? how to handle retries of this part?
	}

	@Override
	public void rollback(TransactionStatus status) throws TransactionException {
		// todo gp same as commit()
	}
}
