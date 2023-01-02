/*
 * Copyright 2017-2023 the original author or authors.
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
package org.springframework.data.couchbase.repository.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.springframework.data.couchbase.config.BeanNames.COUCHBASE_TEMPLATE;

import java.lang.reflect.Method;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserRepository;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;
import org.springframework.data.repository.query.DefaultParameters;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.java.env.ClusterEnvironment;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
class StringN1qlQueryCreatorMockedTests {

	MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> context;
	CouchbaseConverter converter;
	static NamedQueries namedQueries = new PropertiesBasedNamedQueries(new Properties());

	@BeforeEach
	public void beforeEach() {
		context = new CouchbaseMappingContext();
		converter = new MappingCouchbaseConverter(context);
	}

	@Test
	void wrongNumberArgs() throws Exception {
		String input = "getByFirstnameOrLastname";
		Method method = UserRepository.class.getMethod(input, String.class, String.class);

		CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method,
				new DefaultRepositoryMetadata(UserRepository.class), new SpelAwareProxyProjectionFactory(),
				converter.getMappingContext());

		try {
			StringN1qlQueryCreator creator = new StringN1qlQueryCreator(getAccessor(getParameters(method), "Oliver"),
					queryMethod, converter, new SpelExpressionParser(), QueryMethodEvaluationContextProvider.DEFAULT,
					namedQueries);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail("should have failed with IllegalArgumentException: Invalid number of parameters given!");
	}

	@Test
	void doesNotHaveAnnotation() throws Exception {
		String input = "findByFirstname";
		Method method = UserRepository.class.getMethod(input, String.class);
		CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method,
				new DefaultRepositoryMetadata(UserRepository.class), new SpelAwareProxyProjectionFactory(),
				converter.getMappingContext());

		try {
			StringN1qlQueryCreator creator = new StringN1qlQueryCreator(getAccessor(getParameters(method), "Oliver"),
					queryMethod, converter, new SpelExpressionParser(), QueryMethodEvaluationContextProvider.DEFAULT,
					namedQueries);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail("should have failed with IllegalArgumentException: query has no inline Query or named Query not found");
	}

	private ParameterAccessor getAccessor(Parameters<?, ?> params, Object... values) {
		return new ParametersParameterAccessor(params, values);
	}

	private Parameters<?, ?> getParameters(Method method) {
		return new DefaultParameters(method);
	}

}
