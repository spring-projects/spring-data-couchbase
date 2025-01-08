/*
 * Copyright 2012-2025 the original author or authors
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
package org.springframework.data.couchbase.transaction;

import org.springframework.transaction.support.DefaultTransactionStatus;

/**
 * Couchbase transaction status for Spring Data transaction framework.
 *
 * @author Graham Pople
 */
public class CouchbaseTransactionStatus extends DefaultTransactionStatus {

  /**
   * Create a new {@code DefaultTransactionStatus} instance.
   *
   * @param transaction        underlying transaction object that can hold state
   *                           for the internal transaction implementation
   * @param newTransaction     if the transaction is new, otherwise participating
   *                           in an existing transaction
   * @param newSynchronization if a new transaction synchronization has been
   *                           opened for the given transaction
   * @param readOnly           whether the transaction is marked as read-only
   * @param debug              should debug logging be enabled for the handling of this transaction?
   *                           Caching it in here can prevent repeated calls to ask the logging system whether
   *                           debug logging should be enabled.
   * @param suspendedResources a holder for resources that have been suspended
   */
  public CouchbaseTransactionStatus(Object transaction, boolean newTransaction, boolean newSynchronization, boolean readOnly, boolean debug, Object suspendedResources) {
    super(transaction,
        newTransaction,
        newSynchronization,
        readOnly,
        debug,
        suspendedResources);
  }
}
