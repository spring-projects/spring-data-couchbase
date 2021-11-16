package org.springframework.data.couchbase.transaction;

import com.couchbase.transactions.AttemptContext;
import com.couchbase.transactions.AttemptContextReactive;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionDefinition;

public class CouchbaseTransactionDefinition extends DefaultTransactionDefinition {

  AttemptContextReactive atr;
  AttemptContext at;

  public CouchbaseTransactionDefinition(){
    super();
  }

  public CouchbaseTransactionDefinition(TransactionDefinition that) {
    super(that);
  }

  public CouchbaseTransactionDefinition(int propagationBehavior) {
    super(propagationBehavior);
  }

  public void setAttemptContextReactive(AttemptContextReactive atr){
    this.atr = atr;
  }

  public AttemptContextReactive getAttemptContextReactive(){
    return atr;
  }

  public void setAttemptContext(AttemptContext attemptContext) {
    at = attemptContext;
  }
}
