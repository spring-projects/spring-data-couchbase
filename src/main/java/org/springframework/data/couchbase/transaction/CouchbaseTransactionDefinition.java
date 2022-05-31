package org.springframework.data.couchbase.transaction;

import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import org.springframework.transaction.support.DefaultTransactionDefinition;

public class CouchbaseTransactionDefinition extends DefaultTransactionDefinition {

  ReactiveTransactionAttemptContext atr;

  public CouchbaseTransactionDefinition(){
    super();
    setIsolationLevel(ISOLATION_READ_COMMITTED);
  }

  public void setAttemptContextReactive(ReactiveTransactionAttemptContext atr){
    this.atr = atr;
  }
}
