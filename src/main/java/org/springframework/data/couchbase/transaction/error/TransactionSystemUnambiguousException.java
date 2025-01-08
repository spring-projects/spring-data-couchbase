/*
 * Copyright 2022-2025 the original author or authors
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

import com.couchbase.client.java.transactions.error.TransactionFailedException;

/**
 * The transaction failed and unambiguously did not commit. No actors can see any part of this failed transaction.
 * <p>
 * The application does not need to do anything to rollback the transaction.
 *
 * @author Graham Pople
 */
public class TransactionSystemUnambiguousException extends TransactionSystemCouchbaseException {
	public TransactionSystemUnambiguousException(TransactionFailedException ex) {
		super(ex);
	}
}
