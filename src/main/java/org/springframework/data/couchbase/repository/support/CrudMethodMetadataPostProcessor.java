package org.springframework.data.couchbase.repository.support;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.core.NamedThreadLocal;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.support.RepositoryProxyPostProcessor;
import org.springframework.lang.Nullable;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * {@link RepositoryProxyPostProcessor} that sets up interceptors to read metadata information from the invoked method.
 * This is necessary to allow redeclaration of CRUD methods in repository interfaces and configure locking information
 * or query hints on them.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Jens Schauder
 */
class CrudMethodMetadataPostProcessor implements RepositoryProxyPostProcessor, BeanClassLoaderAware {

  private @Nullable ClassLoader classLoader = ClassUtils.getDefaultClassLoader();

  /*
   * (non-Javadoc)
   * @see org.springframework.beans.factory.BeanClassLoaderAware#setBeanClassLoader(java.lang.ClassLoader)
   */
  @Override
  public void setBeanClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  /*
   * (non-Javadoc)
   * @see org.springframework.data.repository.core.support.RepositoryProxyPostProcessor#postProcess(org.springframework.aop.framework.ProxyFactory, org.springframework.data.repository.core.RepositoryInformation)
   */
  @Override
  public void postProcess(ProxyFactory factory, RepositoryInformation repositoryInformation) {
    factory.addAdvice(new CrudMethodMetadataPopulatingMethodInterceptor(repositoryInformation));
  }

  /**
   * Returns a {@link CrudMethodMetadata} proxy that will lookup the actual target object by obtaining a thread bound
   * instance from the {@link TransactionSynchronizationManager} later.
   */
  CrudMethodMetadata getCrudMethodMetadata() {
    ProxyFactory factory = new ProxyFactory();
    factory.addInterface(CrudMethodMetadata.class);
    factory.setTargetSource(new ThreadBoundTargetSource());
    return (CrudMethodMetadata) factory.getProxy(this.classLoader);
  }

  /**
   * {@link MethodInterceptor} to build and cache {@link DefaultCrudMethodMetadata} instances for the invoked methods.
   * Will bind the found information to a {@link TransactionSynchronizationManager} for later lookup.
   *
   * @see DefaultCrudMethodMetadata
   * @author Oliver Gierke
   * @author Thomas Darimont
   */
  static class CrudMethodMetadataPopulatingMethodInterceptor implements MethodInterceptor {

    private static final ThreadLocal<MethodInvocation> currentInvocation = new NamedThreadLocal<>(
      "Current AOP method invocation");

    private final ConcurrentMap<Method, CrudMethodMetadata> metadataCache = new ConcurrentHashMap<>();
    private final Set<Method> implementations = new HashSet<>();

    CrudMethodMetadataPopulatingMethodInterceptor(RepositoryInformation repositoryInformation) {
      ReflectionUtils.doWithMethods(repositoryInformation.getRepositoryInterface(), implementations::add,
        method -> !repositoryInformation.isQueryMethod(method));
    }

    /**
     * Return the AOP Alliance {@link MethodInvocation} object associated with the current invocation.
     *
     * @return the invocation object associated with the current invocation.
     * @throws IllegalStateException if there is no AOP invocation in progress, or if the
     *           {@link CrudMethodMetadataPopulatingMethodInterceptor} was not added to this interceptor chain.
     */
    static MethodInvocation currentInvocation() throws IllegalStateException {

      MethodInvocation mi = currentInvocation.get();

      if (mi == null)
        throw new IllegalStateException(
          "No MethodInvocation found: Check that an AOP invocation is in progress, and that the "
            + "CrudMethodMetadataPopulatingMethodInterceptor is upfront in the interceptor chain.");
      return mi;
    }

    /*
     * (non-Javadoc)
     * @see org.aopalliance.intercept.MethodInterceptor#invoke(org.aopalliance.intercept.MethodInvocation)
     */
    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {

      Method method = invocation.getMethod();

      if (!implementations.contains(method)) {
        return invocation.proceed();
      }

      MethodInvocation oldInvocation = currentInvocation.get();
      currentInvocation.set(invocation);

      try {

        CrudMethodMetadata metadata = (CrudMethodMetadata) TransactionSynchronizationManager.getResource(method);

        if (metadata != null) {
          return invocation.proceed();
        }

        CrudMethodMetadata methodMetadata = metadataCache.get(method);

        if (methodMetadata == null) {

          methodMetadata = new DefaultCrudMethodMetadata(method);
          CrudMethodMetadata tmp = metadataCache.putIfAbsent(method, methodMetadata);

          if (tmp != null) {
            methodMetadata = tmp;
          }
        }

        TransactionSynchronizationManager.bindResource(method, methodMetadata);

        try {
          return invocation.proceed();
        } finally {
          TransactionSynchronizationManager.unbindResource(method);
        }
      } finally {
        currentInvocation.set(oldInvocation);
      }
    }
  }

  /**
   * Default implementation of {@link CrudMethodMetadata} that will inspect the backing method for annotations.
   *
   * @author Oliver Gierke
   * @author Thomas Darimont
   */
  private static class DefaultCrudMethodMetadata implements CrudMethodMetadata {

    private final Method method;

    /**
     * Creates a new {@link DefaultCrudMethodMetadata} for the given {@link Method}.
     *
     * @param method must not be {@literal null}.
     */
    DefaultCrudMethodMetadata(Method method) {
      Assert.notNull(method, "Method must not be null!");
      this.method = method;
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.jpa.repository.support.CrudMethodMetadata#getMethod()
     */
    @Override
    public Method getMethod() {
      return method;
    }
  }

  private static class ThreadBoundTargetSource implements TargetSource {

    /*
     * (non-Javadoc)
     * @see org.springframework.aop.TargetSource#getTargetClass()
     */
    @Override
    public Class<?> getTargetClass() {
      return CrudMethodMetadata.class;
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.aop.TargetSource#isStatic()
     */
    @Override
    public boolean isStatic() {
      return false;
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.aop.TargetSource#getTarget()
     */
    @Override
    public Object getTarget() {

      MethodInvocation invocation = CrudMethodMetadataPopulatingMethodInterceptor.currentInvocation();
      return TransactionSynchronizationManager.getResource(invocation.getMethod());
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.aop.TargetSource#releaseTarget(java.lang.Object)
     */
    @Override
    public void releaseTarget(Object target) {}
  }
}
