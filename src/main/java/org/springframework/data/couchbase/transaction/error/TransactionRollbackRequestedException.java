/*
 * Copyright 2022 the original author or authors
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

import com.couchbase.client.core.error.CouchbaseException;

/**
 * A transaction rollback has been requested.
 *
 * @author Graham Pople
 */
public class TransactionRollbackRequestedException extends CouchbaseException {
	public TransactionRollbackRequestedException(String message) {
		super(message);
	}
}
