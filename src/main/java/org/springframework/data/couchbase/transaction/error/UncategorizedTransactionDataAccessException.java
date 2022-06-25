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

import org.springframework.dao.UncategorizedDataAccessException;

import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
//import com.couchbase.client.core.error.transaction.internal.WrappedTransactionOperationFailedException;

/**
 * An opaque signal that something went wrong during the execution of an operation inside a transaction.
 * <p>
 * The application is not expected to catch or inspect this exception, and should allow it to propagate.
 * <p>
 * Internal state has been set that ensures that the transaction will act appropriately (including rolling
 * back and retrying if necessary) regardless of what the application does with this exception.
 */
public class UncategorizedTransactionDataAccessException extends UncategorizedDataAccessException implements WrappedTransactionOperationFailedException  {
    private final TransactionOperationFailedException internal;

    public UncategorizedTransactionDataAccessException(TransactionOperationFailedException err) {
        super(err.getMessage(), err.getCause());
        this.internal = err;
    }

    @Override
    public TransactionOperationFailedException wrapped() {
        return internal;
    }
}
