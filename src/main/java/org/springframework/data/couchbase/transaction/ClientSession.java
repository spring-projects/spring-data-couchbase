package org.springframework.data.couchbase.transaction;


import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.Scope;
import com.couchbase.transactions.AttemptContext;
import com.couchbase.transactions.AttemptContextReactive;
import com.couchbase.transactions.TransactionGetResult;
import com.couchbase.transactions.TransactionQueryOptions;
import com.couchbase.transactions.config.TransactionConfig;
import org.reactivestreams.Publisher;
import org.springframework.data.couchbase.repository.support.TransactionResultHolder;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * ClientSession. There is only one implementation - ClientSessionImpl
 * The SpringTransaction framework relies on the client session to perform commit() and abort()
 * and therefore it has an AttemptContextReactive
 *
 * @author Michael Reiche
 */
public interface ClientSession /*extends com.mongodb.session.ClientSession*/ {

 Mono<Scope> getScope();

  //Mono<Scope> getScopeReactive();

  boolean hasActiveTransaction();

  boolean notifyMessageSent();

  void notifyOperationInitiated(Object var1);

  //void setAttemptContextReactive(AttemptContextReactive atr);

  AttemptContextReactive getAttemptContextReactive();

  TransactionOptions getTransactionOptions();

  AsyncCluster getWrapped();

  void startTransaction();

  void startTransaction(TransactionConfig var1);

  Publisher<Void> commitTransaction();

  Publisher<Void> abortTransaction();

  ServerSession getServerSession();

  void close();

  Object getClusterTime();

  Object isCausallyConsistent();

 <T> T transactionResultHolder(TransactionResultHolder result, T o);

 TransactionResultHolder transactionResultHolder(Integer key);

  AttemptContext getAttemptContext();

  //ClientSession with(AttemptContextReactive atr);
}
