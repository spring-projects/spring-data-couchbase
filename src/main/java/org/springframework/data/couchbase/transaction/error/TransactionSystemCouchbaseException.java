/*
 * Copyright 2022-2023 the original author or authors
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
package org.springframework.data.couchbase.transaction.error;

import java.util.List;

import org.springframework.transaction.TransactionSystemException;

import com.couchbase.client.core.cnc.events.transaction.TransactionLogEvent;
import com.couchbase.client.java.transactions.error.TransactionFailedException;

/**
 * A base class of transaction-level exceptions raised by Couchbase, allowing them to be handled in one place.
 *
 * @author Graham Pople
 */
abstract public class TransactionSystemCouchbaseException extends TransactionSystemException {
	private final TransactionFailedException internal;

	public TransactionSystemCouchbaseException(TransactionFailedException ex) {
		super(ex.getMessage(), ex.getCause());
		this.internal = ex;
	}

	/**
	 * An in-memory log is built up during each transaction. The application may want to write this to their own logs, for
	 * example upon transaction failure.
	 */
	public List<TransactionLogEvent> logs() {
		return internal.logs();
	}
}
