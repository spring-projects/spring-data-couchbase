package org.springframework.data.couchbase.transaction;

import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import org.springframework.transaction.support.DefaultTransactionDefinition;

public class CouchbaseTransactionDefinition extends DefaultTransactionDefinition {
  public CouchbaseTransactionDefinition(){
    super();
    setIsolationLevel(ISOLATION_READ_COMMITTED);
  }
}
