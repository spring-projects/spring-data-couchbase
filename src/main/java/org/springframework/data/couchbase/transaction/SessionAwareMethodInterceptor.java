/*
 * Copyright 2018-2021 the original author or authors.
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
package org.springframework.data.couchbase.transaction;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.function.BiFunction;

import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.core.MethodClassKey;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ConcurrentReferenceHashMap;
import org.springframework.util.ReflectionUtils;

/**
 * {@link MethodInterceptor} implementation looking up and invoking an alternative target method having
 * {@link CoreTransactionAttemptContext} as its first argument. This allows seamless integration with the existing code base.
 * <br />
 * The {@link MethodInterceptor} is aware of methods on {@code MongoCollection} that my return new instances of itself
 * like (eg. TODO) and decorate them
 * if not already proxied.
 *
 * @param <D> Type of the actual Mongo Database.
 * @param <C> Type of the actual Mongo Collection.
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
public class SessionAwareMethodInterceptor<D, C> implements MethodInterceptor {

  private static final MethodCache METHOD_CACHE = new MethodCache();

  private final ReactiveCouchbaseResourceHolder session;
  private final ReactiveCouchbaseResourceHolderOperator collectionDecorator;
  private final ReactiveCouchbaseResourceHolderOperator databaseDecorator;
  private final Object target;
  private final Class<?> targetType;
  private final Class<?> collectionType;
  private final Class<?> databaseType;
  private final Class<? extends ReactiveCouchbaseResourceHolder> sessionType;

  /**
   * Create a new SessionAwareMethodInterceptor for given target.
   *
   * @param session the {@link CoreTransactionAttemptContext} to be used on invocation.
   * @param target the original target object.
   * @param databaseType the MongoDB database type
   * @param databaseDecorator a {@link ReactiveCouchbaseResourceHolderOperator} used to create the proxy for an imperative / reactive
   *          {@code MongoDatabase}.
   * @param collectionType the MongoDB collection type.
   * @param collectionDecorator a {@link ReactiveCouchbaseResourceHolderOperator} used to create the proxy for an imperative / reactive
   *          {@code MongoCollection}.
   * @param <T> target object type.
   */
  public <T> SessionAwareMethodInterceptor(ReactiveCouchbaseResourceHolder session, T target, Class<? extends ReactiveCouchbaseResourceHolder> sessionType,
                                           Class<D> databaseType, ReactiveCouchbaseResourceHolderOperator<D> databaseDecorator, Class<C> collectionType,
                                           ReactiveCouchbaseResourceHolderOperator<C> collectionDecorator) {

    Assert.notNull(session, "CoreTransactionAttemptContext must not be null!");
    Assert.notNull(target, "Target must not be null!");
    Assert.notNull(sessionType, "SessionType must not be null!");
    Assert.notNull(databaseType, "Database type must not be null!");
    Assert.notNull(databaseDecorator, "Database CoreTransactionAttemptContextOperator must not be null!");
    Assert.notNull(collectionType, "Collection type must not be null!");
    Assert.notNull(collectionDecorator, "Collection CoreTransactionAttemptContextOperator must not be null!");

    this.session = session;
    this.target = target;
    this.databaseType = ClassUtils.getUserClass(databaseType);
    this.collectionType = ClassUtils.getUserClass(collectionType);
    this.collectionDecorator = collectionDecorator;
    this.databaseDecorator = databaseDecorator;

    this.targetType = ClassUtils.isAssignable(databaseType, target.getClass()) ? databaseType : collectionType;
    this.sessionType = sessionType;
  }

  /*
   * (non-Javadoc)
   * @see org.aopalliance.intercept.MethodInterceptor(org.aopalliance.intercept.MethodInvocation)
   */
  @Nullable
  @Override
  public Object invoke(MethodInvocation methodInvocation) throws Throwable {

    if (requiresDecoration(methodInvocation.getMethod())) {

      Object target = methodInvocation.proceed();
      if (target instanceof Proxy) {
        return target;
      }

      return decorate(target);
    }

    if (!requiresSession(methodInvocation.getMethod())) {
      return methodInvocation.proceed();
    }

    Optional<Method> targetMethod = METHOD_CACHE.lookup(methodInvocation.getMethod(), targetType, sessionType);

    return !targetMethod.isPresent() ? methodInvocation.proceed()
            : ReflectionUtils.invokeMethod(targetMethod.get(), target,
            prependSessionToArguments(session, methodInvocation));
  }

  private boolean requiresDecoration(Method method) {

    return ClassUtils.isAssignable(databaseType, method.getReturnType())
            || ClassUtils.isAssignable(collectionType, method.getReturnType());
  }

  @SuppressWarnings("unchecked")
  protected Object decorate(Object target) {

    return ClassUtils.isAssignable(databaseType, target.getClass()) ? databaseDecorator.apply(session, target)
            : collectionDecorator.apply(session, target);
  }

  private static boolean requiresSession(Method method) {

    if (method.getParameterCount() == 0
            || !ClassUtils.isAssignable(CoreTransactionAttemptContext.class, method.getParameterTypes()[0])) {
      return true;
    }

    return false;
  }

  private static Object[] prependSessionToArguments(ReactiveCouchbaseResourceHolder session, MethodInvocation invocation) {

    Object[] args = new Object[invocation.getArguments().length + 1];

    args[0] = session;
    System.arraycopy(invocation.getArguments(), 0, args, 1, invocation.getArguments().length);

    return args;
  }

  /**
   * Simple {@link Method} to {@link Method} caching facility for {@link CoreTransactionAttemptContext} overloaded targets.
   *
   * @since 2.1
   * @author Christoph Strobl
   */
  static class MethodCache {

    private final ConcurrentReferenceHashMap<MethodClassKey, Optional<Method>> cache = new ConcurrentReferenceHashMap<>();

    /**
     * Lookup the target {@link Method}.
     *
     * @param method
     * @param targetClass
     * @return
     */
    Optional<Method> lookup(Method method, Class<?> targetClass, Class<? extends ReactiveCouchbaseResourceHolder> sessionType) {

      return cache.computeIfAbsent(new MethodClassKey(method, targetClass),
              val -> Optional.ofNullable(findTargetWithSession(method, targetClass, sessionType)));
    }

    @Nullable
    private Method findTargetWithSession(Method sourceMethod, Class<?> targetType,
                                         Class<? extends ReactiveCouchbaseResourceHolder> sessionType) {

      Class<?>[] argTypes = sourceMethod.getParameterTypes();
      Class<?>[] args = new Class<?>[argTypes.length + 1];
      args[0] = sessionType;
      System.arraycopy(argTypes, 0, args, 1, argTypes.length);

      return ReflectionUtils.findMethod(targetType, sourceMethod.getName(), args);
    }

    /**
     * Check whether the cache contains an entry for {@link Method} and {@link Class}.
     *
     * @param method
     * @param targetClass
     * @return
     */
    boolean contains(Method method, Class<?> targetClass) {
      return cache.containsKey(new MethodClassKey(method, targetClass));
    }
  }

  /**
   * Represents an operation upon two operands of the same type, producing a result of the same type as the operands
   * accepting {@link CoreTransactionAttemptContext}. This is a specialization of {@link BiFunction} for the case where the operands and
   * the result are all of the same type.
   *
   * @param <T> the type of the operands and result of the operator
   */
  public interface ReactiveCouchbaseResourceHolderOperator<T> extends BiFunction<ReactiveCouchbaseResourceHolder, T, T> {}
}
