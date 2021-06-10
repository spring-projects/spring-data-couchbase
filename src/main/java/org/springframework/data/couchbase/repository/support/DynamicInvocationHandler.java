/*
 * Copyright 2021 the original author or authors.
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
package org.springframework.data.couchbase.repository.support;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;

import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.ReactiveCouchbaseRepository;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;

import com.couchbase.client.java.CommonOptions;

/**
 * Invocation Handler for scope/collection/options proxy for repositories
 *
 * @param <T1>
 *
 * @author Michael Reiche
 *
 */
public class DynamicInvocationHandler<T1> implements InvocationHandler {
	final T1 target;
	final Class<?> repositoryClass;
	final CouchbaseEntityInformation<?, String> entityInformation;
	final ReactiveCouchbaseTemplate reactiveTemplate;
	CommonOptions<?> options;
	String collection;
	String scope;;

	public DynamicInvocationHandler(T1 target, CommonOptions<?> options, String collection, String scope) {
		this.target = target;
		if (target instanceof CouchbaseRepository) {
			reactiveTemplate = ((CouchbaseTemplate) ((CouchbaseRepository) target).getOperations()).reactive();
		} else if (target instanceof ReactiveCouchbaseRepository) {
			reactiveTemplate = (ReactiveCouchbaseTemplate) ((ReactiveCouchbaseRepository) target).getOperations();
		} else {
			throw new RuntimeException("Unknown target type: " + target.getClass());
		}
		this.entityInformation = target instanceof CouchbaseRepository
				? ((CouchbaseRepository<?, String>) target).getEntityInformation()
				: ((ReactiveCouchbaseRepository<?, String>) target).getEntityInformation();
		this.options = options;
		this.collection = collection;
		this.scope = scope;
		this.repositoryClass = target.getClass();

	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

		if ("toString".equals(method.getName())) {
			return "proxy -> target:" + target;
		}
		/* Cannot fall-through to use these methods on target, as they will not retain
		 * the scope, collection and options that may already be set on the proxy
		 */

		if (method.getName().equals("withOptions")) {
			return Proxy.newProxyInstance(this.getClass().getClassLoader(), target.getClass().getInterfaces(),
					new DynamicInvocationHandler<>(target, (CommonOptions) args[0], collection, scope));
		}

		if (method.getName().equals("withScope")) {
			return Proxy.newProxyInstance(this.getClass().getClassLoader(), target.getClass().getInterfaces(),
					new DynamicInvocationHandler<>(target, options, collection, (String) args[0]));
		}

		if (method.getName().equals("withCollection")) {
			return Proxy.newProxyInstance(this.getClass().getClassLoader(), target.getClass().getInterfaces(),
					new DynamicInvocationHandler<>(target, options, (String) args[0], scope));
		}

		Class<?>[] paramTypes = null;
		if (args != null) {
			// the CouchbaseRepository methods - save(entity) etc - will have a parameter type of Object instead of entityType
			// so change the paramType to match
			paramTypes = Arrays.stream(args)
					.map(o -> o == null ? null : (o.getClass() == entityInformation.getJavaType() ? Object.class : o.getClass()))
					.toArray(Class<?>[]::new);
		}

		Method theMethod = repositoryClass.getMethod(method.getName(), paramTypes);
		Object result;

		try {
			setThreadLocal();
			result = theMethod.invoke(target, args);
		} catch (InvocationTargetException ite) {
			throw ite.getCause();
		} finally {
			clearThreadLocal();
		}
		return result;
	}

	private void setThreadLocal() {
		reactiveTemplate.setPseudoArgs(new PseudoArgs(this.scope, this.collection, this.options));
	}

	private void clearThreadLocal() {
		reactiveTemplate.setPseudoArgs(null);
	}
}
