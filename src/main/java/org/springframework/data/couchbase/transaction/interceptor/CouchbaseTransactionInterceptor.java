/*
 * Copyright 2002-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.transaction.interceptor;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.core.ReactiveAdapterRegistry;
import org.springframework.lang.Nullable;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.TransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.interceptor.TransactionProxyFactoryBean;
import org.springframework.transaction.support.CallbackPreferringPlatformTransactionManager;
import org.springframework.util.ClassUtils;
import org.springframework.util.ConcurrentReferenceHashMap;

/**
 * AOP Alliance MethodInterceptor for declarative transaction
 * management using the common Spring transaction infrastructure
 * ({@link org.springframework.transaction.PlatformTransactionManager}/
 * {@link org.springframework.transaction.ReactiveTransactionManager}).
 *
 * <p>Derives from the {@link TransactionAspectSupport} class which
 * contains the integration with Spring's underlying transaction API.
 * TransactionInterceptor simply calls the relevant superclass methods
 * such as {@link #invokeWithinTransaction} in the correct order.
 *
 * <p>TransactionInterceptors are thread-safe.
 *
 * @author Rod Johnson
 * @author Juergen Hoeller
 * @author Sebastien Deleuze
 * @see TransactionProxyFactoryBean
 * @see org.springframework.aop.framework.ProxyFactoryBean
 * @see org.springframework.aop.framework.ProxyFactory
 */
@SuppressWarnings("serial")
public class CouchbaseTransactionInterceptor extends TransactionInterceptor implements MethodInterceptor, Serializable {

// NOTE: This class must not implement Serializable because it serves as base
  // class for AspectJ aspects (which are not allowed to implement Serializable)!

  /**
   * Vavr library present on the classpath?
   */
  private static final boolean vavrPresent = ClassUtils.isPresent(
      "io.vavr.control.Try", TransactionAspectSupport.class.getClassLoader());

  /**
   * Reactive Streams API present on the classpath?
   */
  private static final boolean reactiveStreamsPresent =
      ClassUtils.isPresent("org.reactivestreams.Publisher", TransactionAspectSupport.class.getClassLoader());

  protected final Log logger = LogFactory.getLog(getClass());

  @Nullable
  private final ReactiveAdapterRegistry reactiveAdapterRegistry;

  private final ConcurrentMap<Object, TransactionManager> transactionManagerCache =
      new ConcurrentReferenceHashMap<>(4);

  /**
   * Create a new TransactionInterceptor.
   * <p>Transaction manager and transaction attributes still need to be set.
   * @see #setTransactionManager
   * @see #setTransactionAttributes(java.util.Properties)
   * @see #setTransactionAttributeSource(TransactionAttributeSource)
   */
  public CouchbaseTransactionInterceptor() {
    if (reactiveStreamsPresent) {
      this.reactiveAdapterRegistry = ReactiveAdapterRegistry.getSharedInstance();
    }
    else {
      this.reactiveAdapterRegistry = null;
    }
  }

  /**
   * Create a new TransactionInterceptor.
   * @param ptm the default transaction manager to perform the actual transaction management
   * @param tas the attribute source to be used to find transaction attributes
   * @since 5.2.5
   * @see #setTransactionManager
   * @see #setTransactionAttributeSource
   */
  public CouchbaseTransactionInterceptor(TransactionManager ptm, TransactionAttributeSource tas) {
    this();
    setTransactionManager(ptm);
    setTransactionAttributeSource(tas);
  }

  /**
   * Create a new TransactionInterceptor.
   * @param ptm the default transaction manager to perform the actual transaction management
   * @param tas the attribute source to be used to find transaction attributes
   * @see #setTransactionManager
   * @see #setTransactionAttributeSource
   * @deprecated as of 5.2.5, in favor of
   * {@link #CouchbaseTransactionInterceptor(TransactionManager, TransactionAttributeSource)}
   */
  @Deprecated
  public CouchbaseTransactionInterceptor(PlatformTransactionManager ptm, TransactionAttributeSource tas) {
    this();
    setTransactionManager(ptm);
    setTransactionAttributeSource(tas);
  }

  /**
   * Create a new TransactionInterceptor.
   * @param ptm the default transaction manager to perform the actual transaction management
   * @param attributes the transaction attributes in properties format
   * @see #setTransactionManager
   * @see #setTransactionAttributes(java.util.Properties)
   * @deprecated as of 5.2.5, in favor of {@link #setTransactionAttributes(Properties)}
   */
  @Deprecated
  public CouchbaseTransactionInterceptor(PlatformTransactionManager ptm, Properties attributes) {
    this();
    setTransactionManager(ptm);
    setTransactionAttributes(attributes);
  }


  @Override
  @Nullable
  public Object invoke(MethodInvocation invocation) throws Throwable {
    // Work out the target class: may be {@code null}.
    // The TransactionAttributeSource should be passed the target class
    // as well as the method, which may be from an interface.
    Class<?> targetClass = (invocation.getThis() != null ? AopUtils.getTargetClass(invocation.getThis()) : null);

    // Adapt to TransactionAspectSupport's invokeWithinTransaction...
    return invokeWithinTransaction(invocation.getMethod(), targetClass, new CoroutinesInvocationCallback() {
      @Override
      @Nullable
      public Object proceedWithInvocation() throws Throwable {
        return invocation.proceed();
      }
      @Override
      public Object getTarget() {
        return invocation.getThis();
      }
      @Override
      public Object[] getArguments() {
        return invocation.getArguments();
      }
    });
  }

  /**
   * General delegate for around-advice-based subclasses, delegating to several other template
   * methods on this class. Able to handle {@link CallbackPreferringPlatformTransactionManager}
   * as well as regular {@link PlatformTransactionManager} implementations and
   * {@link ReactiveTransactionManager} implementations for reactive return types.
   * @param method the Method being invoked
   * @param targetClass the target class that we're invoking the method on
   * @param invocation the callback to use for proceeding with the target invocation
   * @return the return value of the method, if any
   * @throws Throwable propagated from the target invocation
   */
  @Nullable
  protected Object invokeWithinTransaction(Method method, @Nullable Class<?> targetClass,
                                           final InvocationCallback invocation) throws Throwable {

    // If the transaction attribute is null, the method is non-transactional.
    TransactionAttributeSource tas = getTransactionAttributeSource();
    final TransactionAttribute txAttr = (tas != null ? tas.getTransactionAttribute(method, targetClass) : null);
    final TransactionManager tm = determineTransactionManager(txAttr);

    if (this.reactiveAdapterRegistry != null && tm instanceof ReactiveTransactionManager) {
        return super.invokeWithinTransaction(method, targetClass, invocation);
    }

    PlatformTransactionManager ptm = asPlatformTransactionManager(tm);

    if (txAttr == null || !(ptm instanceof CallbackPreferringPlatformTransactionManager)) {
      return super.invokeWithinTransaction(method, targetClass, invocation);
    }

    else {
      final String joinpointIdentification = methodIdentification(method, targetClass, txAttr);

      Object result;
      final ThrowableHolder throwableHolder = new ThrowableHolder();

      // It's a CallbackPreferringPlatformTransactionManager: pass a TransactionCallback in.
      try {
        result = ((CallbackPreferringPlatformTransactionManager) ptm).execute(txAttr, status -> {
          TransactionInfo txInfo = prepareTransactionInfo(ptm, txAttr, joinpointIdentification, status);
          try {
            Object retVal = invocation.proceedWithInvocation();
            if (retVal != null && vavrPresent && VavrDelegate.isVavrTry(retVal)) {
              // Set rollback-only in case of Vavr failure matching our rollback rules...
              retVal = VavrDelegate.evaluateTryFailure(retVal, txAttr, status);
            }
            return retVal;
          }
          catch (Throwable ex) {
            if (txAttr.rollbackOn(ex)) {
              // A RuntimeException: will lead to a rollback.
              if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
              }
              else {
                throw new ThrowableHolderException(ex);
              }
            }
            else {
              // A normal return value: will lead to a commit.
              throwableHolder.throwable = ex;
              return null;
            }
          }
          finally {
            cleanupTransactionInfo(txInfo);
          }
        });
      }
      catch (ThrowableHolderException ex) {
        throw ex.getCause();
      }
      catch (TransactionSystemException ex2) {
        if (throwableHolder.throwable != null) {
          logger.error("Application exception overridden by commit exception", throwableHolder.throwable);
          ex2.initApplicationException(throwableHolder.throwable);
        }
        throw ex2;
      }
      catch (Throwable ex2) {
        if (throwableHolder.throwable != null) {
          logger.error("Application exception overridden by commit exception", throwableHolder.throwable);
        }
        throw ex2;
      }

      // Check result state: It might indicate a Throwable to rethrow.
      if (throwableHolder.throwable != null) {
        throw throwableHolder.throwable;
      }
      return result;
    }
  }

  // from TransactionAspectSupport
  @Nullable
  private PlatformTransactionManager asPlatformTransactionManager(@Nullable Object transactionManager) {
    if (transactionManager == null || transactionManager instanceof PlatformTransactionManager) {
      return (PlatformTransactionManager) transactionManager;
    }
    else {
      throw new IllegalStateException(
          "Specified transaction manager is not a PlatformTransactionManager: " + transactionManager);
    }
  }

  // from TransactionAspectSupport
  private String methodIdentification(Method method, @Nullable Class<?> targetClass,
                                      @Nullable TransactionAttribute txAttr) {

    String methodIdentification = methodIdentification(method, targetClass);
    if (methodIdentification == null) {
      if (txAttr instanceof DefaultTransactionAttribute) {
        methodIdentification = ((DefaultTransactionAttribute) txAttr).getDescriptor();
      }
      if (methodIdentification == null) {
        methodIdentification = ClassUtils.getQualifiedMethodName(method, targetClass);
      }
    }
    return methodIdentification;
  }

  // from TransactionAspectSupport
  /**
   * Internal holder class for a Throwable, used as a RuntimeException to be
   * thrown from a TransactionCallback (and subsequently unwrapped again).
   */
  @SuppressWarnings("serial")
  private static class ThrowableHolderException extends RuntimeException {

    public ThrowableHolderException(Throwable throwable) {
      super(throwable);
    }

    @Override
    public String toString() {
      return getCause().toString();
    }
  }

  // from TransactionAspectSupport
  /**
   * Internal holder class for a Throwable in a callback transaction model.
   */
  private static class ThrowableHolder {

    @Nullable
    public Throwable throwable;
  }

  // From TransactionAspectSupport
  /**
   * Inner class to avoid a hard dependency on the Vavr library at runtime.
   */
  private static class VavrDelegate {

    public static boolean isVavrTry(Object retVal) {
      return false; // (retVal instanceof Try);
    }

    public static Object evaluateTryFailure(Object retVal, TransactionAttribute txAttr, TransactionStatus status) {
      throw new RuntimeException("no Vavr support");
      /*
      return ((Try<?>) retVal).onFailure(ex -> {
        if (txAttr.rollbackOn(ex)) {
          status.setRollbackOnly();
        }
      });
       */
    }
  }

  //---------------------------------------------------------------------
  // Serialization support
  //---------------------------------------------------------------------

  private void writeObject(ObjectOutputStream oos) throws IOException {
    // Rely on default serialization, although this class itself doesn't carry state anyway...
    oos.defaultWriteObject();

    // Deserialize superclass fields.
    oos.writeObject(getTransactionManagerBeanName());
    oos.writeObject(getTransactionManager());
    oos.writeObject(getTransactionAttributeSource());
    oos.writeObject(getBeanFactory());
  }

  private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
    // Rely on default serialization, although this class itself doesn't carry state anyway...
    ois.defaultReadObject();

    // Serialize all relevant superclass fields.
    // Superclass can't implement Serializable because it also serves as base class
    // for AspectJ aspects (which are not allowed to implement Serializable)!
    setTransactionManagerBeanName((String) ois.readObject());
    setTransactionManager((PlatformTransactionManager) ois.readObject());
    setTransactionAttributeSource((TransactionAttributeSource) ois.readObject());
    setBeanFactory((BeanFactory) ois.readObject());
  }

}
