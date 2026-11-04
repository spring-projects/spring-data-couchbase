/*
 * Copyright 2022-present the original author or authors
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

import com.couchbase.client.java.transactions.error.TransactionCommitAmbiguousException;

/**
 * The transaction expired at the point of trying to commit it.  It is ambiguous whether the transaction has committed
 * or not.  Actors may be able to see the content of this transaction.
 *
 * This error is the result of inevitable and unavoidable edge cases when working with unreliable networks.  For example,
 * consider an ordinary mutation being made over the network to any database.  The mutation could succeed on the
 * database-side, and then just before the result is returned to the client, the network connection drops.  The client
 * cannot receive the success result and will timeout - it is ambiguous to it whether the mutation succeeded or not.
 *
 * The transactions logic will work to resolve the ambiguity up until the transaction expires, but if unable to resolve
 * it in that time, it is forced to raise this error.  The transaction may or may not have been successful, and
 * error-handling of this is highly application-dependent.
 *
 * An asynchronous cleanup process will try to complete the transaction: roll it back if it didn't commit, roll it
 * forwards if it did.
 *
 * @author Graham Pople
 */
public class TransactionSystemAmbiguousException extends TransactionSystemCouchbaseException {
    public TransactionSystemAmbiguousException(TransactionCommitAmbiguousException ex) {
        super(ex);
    }
}
