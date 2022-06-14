package org.springframework.data.couchbase.transaction;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import org.springframework.transaction.support.DefaultTransactionDefinition;

@Stability.Internal
public class CouchbaseTransactionDefinition extends DefaultTransactionDefinition {
  public CouchbaseTransactionDefinition(){
    super();
    setIsolationLevel(ISOLATION_READ_COMMITTED);
  }
}
