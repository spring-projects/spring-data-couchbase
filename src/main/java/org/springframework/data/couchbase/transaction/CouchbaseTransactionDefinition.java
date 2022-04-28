package org.springframework.data.couchbase.transaction;

import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionAttemptContext;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionDefinition;

public class CouchbaseTransactionDefinition extends DefaultTransactionDefinition {

  ReactiveTransactionAttemptContext atr;
  TransactionAttemptContext at;

  public CouchbaseTransactionDefinition(){
    super();
    setIsolationLevel(ISOLATION_READ_COMMITTED);
  }

  public CouchbaseTransactionDefinition(TransactionDefinition that) {
    super(that);
  }

  public CouchbaseTransactionDefinition(int propagationBehavior) {
    super(propagationBehavior);
  }

  public void setAttemptContextReactive(ReactiveTransactionAttemptContext atr){
    this.atr = atr;
  }

  public ReactiveTransactionAttemptContext getAttemptContextReactive(){
    return atr;
  }

  public void setAttemptContext(TransactionAttemptContext attemptContext) {
    at = attemptContext;
  }
}
