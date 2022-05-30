package org.springframework.data.couchbase.transaction;

import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

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
  public CouchbaseTransactionStatus(Object transaction, boolean newTransaction, boolean newSynchronization, boolean readOnly, boolean debug, Object suspendedResources, TransactionSynchronizationManager sm) {
    super(transaction,
        newTransaction,
        newSynchronization,
        readOnly,
        debug,
        suspendedResources);
  }
}
