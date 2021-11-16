
//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.springframework.data.couchbase.transaction.internal;

import org.springframework.data.couchbase.transaction.ClientSession;
import org.springframework.data.couchbase.transaction.TransactionOptions;

public interface AsyncClientSession extends ClientSession {
  boolean hasActiveTransaction();

  boolean notifyMessageSent();

  TransactionOptions getTransactionOptions();

  void startTransaction();

  void startTransaction(TransactionOptions var1);

  void commitTransaction(SingleResultCallback<Void> var1);

  void abortTransaction(SingleResultCallback<Void> var1);
}
