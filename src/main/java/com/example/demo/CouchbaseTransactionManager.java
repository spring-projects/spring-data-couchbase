package com.example.demo;

import java.util.concurrent.atomic.AtomicReference;

import com.couchbase.transactions.AttemptContextReactive;
import com.couchbase.transactions.AttemptContextReactiveAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.transaction.ClientSession;
import org.springframework.data.couchbase.transaction.ClientSessionImpl;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.CallbackPreferringPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceHolderSupport;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionSynchronizationUtils;
import org.springframework.util.Assert;

import com.couchbase.transactions.AttemptContext;
import com.couchbase.transactions.TransactionResult;
import com.couchbase.transactions.Transactions;
import com.couchbase.transactions.config.TransactionConfig;

public class CouchbaseTransactionManager extends AbstractPlatformTransactionManager
  implements DisposableBean, ResourceTransactionManager, CallbackPreferringPlatformTransactionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTransactionManager.class);

  private final CouchbaseTemplate template;
  private final Transactions transactions;

  public CouchbaseTransactionManager(CouchbaseTemplate template, TransactionConfig transactionConfig) {
    this.template = template;
    this.transactions = Transactions.create(
      template.getCouchbaseClientFactory().getCluster(),
      transactionConfig
    );
  }

  public CouchbaseTransactionalTemplate template() {
    return new CouchbaseTransactionalTemplate(template);
  }

  @Override
  public <T> T execute(TransactionDefinition definition, TransactionCallback<T> callback) throws TransactionException {
    final AtomicReference<T> result = new AtomicReference<>();
    TransactionResult txnResult = transactions.run(attemptContext -> {

      if (TransactionSynchronizationManager.hasResource(template.getCouchbaseClientFactory())) {
        ((CouchbaseResourceHolder) TransactionSynchronizationManager
          .getResource(template.reactive().getCouchbaseClientFactory()))
          .setAttemptContext(attemptContext);
      } else {
        TransactionSynchronizationManager.bindResource(
          template.reactive().getCouchbaseClientFactory(),
          new CouchbaseResourceHolder(attemptContext)
        );
      }

      try {
        // Since we are on a different thread now transparently, at least make sure
        // that the original method invocation is synchronized.
        synchronized (this) {
          result.set(callback.doInTransaction(null));
        }
      } catch (RuntimeException e) {
        System.err.println("RuntimeException: "+e+" instanceof RuntimeException "+(e instanceof RuntimeException));
        throw e;
      } catch (Throwable e) {
        System.err.println("RuntimeException: "+e+" instanceof "+(e instanceof Throwable));
        throw new RuntimeException(e);
      }
    });

    LOGGER.debug("Completed Couchbase Transaction with Result: " + txnResult);
    return result.get();
  }

  @Override
  protected CouchbaseTransactionObject doGetTransaction() throws TransactionException {
    CouchbaseResourceHolder resourceHolder = (CouchbaseResourceHolder) TransactionSynchronizationManager
      .getResource(template.getCouchbaseClientFactory());
    return new CouchbaseTransactionObject(resourceHolder);
  }

  @Override
  protected boolean isExistingTransaction(Object transaction) throws TransactionException {
    return extractTransaction(transaction).hasResourceHolder();
  }

  @Override
  protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
    LOGGER.debug("Beginning Couchbase Transaction with Definition {}", definition);
  }

  @Override
  protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
    LOGGER.debug("Committing Couchbase Transaction with status {}", status);
  }

  @Override
  protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
    LOGGER.warn("Rolling back Couchbase Transaction with status {}", status);
  }

  @Override
  protected void doCleanupAfterCompletion(Object transaction) {
    LOGGER.trace("Performing cleanup of Couchbase Transaction {}", transaction);
  }

  @Override
  public void destroy() {
    transactions.close();
  }

  @Override
  public Object getResourceFactory() {
    return template.getCouchbaseClientFactory();
  }

  private static CouchbaseTransactionObject extractTransaction(Object transaction) {
    Assert.isInstanceOf(CouchbaseTransactionObject.class, transaction,
      () -> String.format("Expected to find a %s but it turned out to be %s.", CouchbaseTransactionObject.class,
        transaction.getClass()));

    return (CouchbaseTransactionObject) transaction;
  }

  public static class CouchbaseResourceHolder extends ResourceHolderSupport {

    private volatile AttemptContext attemptContext;
    private volatile AttemptContextReactive attemptContextReactive;
    private volatile ClientSession session = new ClientSessionImpl();

    public CouchbaseResourceHolder(AttemptContext attemptContext) {
      this.attemptContext = attemptContext;
    }

    public AttemptContext getAttemptContext() {
      return attemptContext;
    }

    public void setAttemptContext(AttemptContext attemptContext) {
      this.attemptContext = attemptContext;
    }

    public AttemptContextReactive getAttemptContextReactive() {
      return attemptContext!= null ? AttemptContextReactiveAccessor.getACR(attemptContext) : attemptContextReactive;
    }
    public void setAttemptContextReactive(AttemptContextReactive attemptContextReactive) {
      this.attemptContextReactive = attemptContextReactive;
    }

    public ClientSession getSession() {
      return session;
    }

    public void setSession(ClientSession session){
      this.session = session;
    }

    @Override
    public String toString() {
      return "CouchbaseResourceHolder{" +
        "attemptContext=" + attemptContext +
        '}';
    }

  }

  protected static class CouchbaseTransactionObject implements SmartTransactionObject {

    final CouchbaseResourceHolder resourceHolder;

    CouchbaseTransactionObject(CouchbaseResourceHolder resourceHolderIn) {
      resourceHolder = resourceHolderIn;
    }

    @Override
    public boolean isRollbackOnly() {
      return resourceHolder != null && resourceHolder.isRollbackOnly();
    }

    @Override
    public void flush() {
      TransactionSynchronizationUtils.triggerFlush();
    }

    public boolean hasResourceHolder() {
      return resourceHolder != null;
    }

    @Override
    public String toString() {
      return "CouchbaseTransactionObject{" +
        "resourceHolder=" + resourceHolder +
        '}';
    }
  }

}