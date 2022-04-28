package org.springframework.data.couchbase.transaction;


import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionAttemptContext;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import org.reactivestreams.Publisher;
import org.springframework.data.couchbase.repository.support.TransactionResultHolder;
import reactor.core.publisher.Mono;

/**
 * ClientSession. There is only one implementation - ClientSessionImpl
 * The SpringTransaction framework relies on the client session to perform commit() and abort()
 * and therefore it has a ReactiveTransactionAttemptContext
 *
 * @author Michael Reiche
 */
// todo gp understand why this is needed
public interface ClientSession /*extends com.mongodb.session.ClientSession*/ {

 Mono<Scope> getScope();

  //Mono<Scope> getScopeReactive();

  boolean hasActiveTransaction();

  boolean notifyMessageSent();

  void notifyOperationInitiated(Object var1);

  //void setAttemptContextReactive(ReactiveTransactionAttemptContext atr);

  ReactiveTransactionAttemptContext getReactiveTransactionAttemptContext();

  TransactionOptions getTransactionOptions();

  AsyncCluster getWrapped();

  void startTransaction();

  Publisher<Void> commitTransaction();

  Publisher<Void> abortTransaction();

  ServerSession getServerSession();

  void close();

  Object getClusterTime();

  Object isCausallyConsistent();

 <T> T transactionResultHolder(TransactionResultHolder result, T o);

 TransactionResultHolder transactionResultHolder(Integer key);

  TransactionAttemptContext getTransactionAttemptContext();

  //ClientSession with(ReactiveTransactionAttemptContext atr);
}
