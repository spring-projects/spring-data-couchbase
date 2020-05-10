/*
 * Copyright 2017-2019 the original author or authors.
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

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.data.couchbase.config.BeanNames.COUCHBASE_TEMPLATE;

import java.lang.reflect.Method;
import java.util.Properties;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ExecutableFindByQueryOperation;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.domain.Config;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserRepository;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;
import org.springframework.data.repository.query.*;

import javax.el.MethodNotFoundException;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
class StringN1qlQueryCreatorTests {

	MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> context;
	CouchbaseConverter converter;
	CouchbaseTemplate couchbaseTemplate;
	static NamedQueries namedQueries = new PropertiesBasedNamedQueries(new Properties());

	@BeforeEach
	public void beforeEach() {
		context = new CouchbaseMappingContext();
		converter = new MappingCouchbaseConverter(context);
		ApplicationContext ac = new AnnotationConfigApplicationContext(Config.class);
		couchbaseTemplate = (CouchbaseTemplate) ac.getBean(COUCHBASE_TEMPLATE);
	}

	@Test
	void createsQueryCorrectly() throws Exception {
		String input = "getByFirstnameAndLastname";
		Method method = UserRepository.class.getMethod(input, String.class, String.class);

		CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method,
				new DefaultRepositoryMetadata(UserRepository.class), new SpelAwareProxyProjectionFactory(),
				converter.getMappingContext());

		StringN1qlQueryCreator creator = new StringN1qlQueryCreator(
				getAccessor(getParameters(method), "Oliver", "Twist"), queryMethod, converter, "travel-sample",
				QueryMethodEvaluationContextProvider.DEFAULT, namedQueries);

		Query query = creator.createQuery();
		assertEquals(
				"SELECT META(`travel-sample`).id AS __id, META(`travel-sample`).cas AS __cas, `travel-sample`.* FROM `travel-sample` where firstname = $1 and lastname = $2 AND `_class` = \"org.springframework.data.couchbase.domain.User\"",
				query.toN1qlString(couchbaseTemplate.reactive(), User.class, false));
	}

	@Test
	void createsQueryCorrectly2() throws Exception {
		String input = "getByFirstnameOrLastname";
		Method method = UserRepository.class.getMethod(input, String.class, String.class);

		CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method,
				new DefaultRepositoryMetadata(UserRepository.class), new SpelAwareProxyProjectionFactory(),
				converter.getMappingContext());

		StringN1qlQueryCreator creator = new StringN1qlQueryCreator(
				getAccessor(getParameters(method), "Oliver", "Twist"), queryMethod, converter, "travel-sample",
				QueryMethodEvaluationContextProvider.DEFAULT, namedQueries);

		Query query = creator.createQuery();
		assertEquals(
				"SELECT META(`travel-sample`).id AS __id, META(`travel-sample`).cas AS __cas, `travel-sample`.* FROM `travel-sample` where (firstname = $first or lastname = $last) AND `_class` = \"org.springframework.data.couchbase.domain.User\"",
				query.toN1qlString(couchbaseTemplate.reactive(), User.class, false));
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
					queryMethod, converter, "travel-sample", QueryMethodEvaluationContextProvider.DEFAULT,
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
					queryMethod, converter, "travel-sample", QueryMethodEvaluationContextProvider.DEFAULT,
					namedQueries);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail("should have failed with IllegalArgumentException: query has no inline Query or named Query not found");
	}

	@Test
	@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
	void findUsingStringNq1l() throws Exception {
		User user = new User(UUID.randomUUID().toString(), "Oliver", "Twist");
		User modified = couchbaseTemplate.upsertById(User.class).one(user);

		String input = "getByFirstnameOrLastname";
		Method method = UserRepository.class.getMethod(input, String.class, String.class);

		CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method,
				new DefaultRepositoryMetadata(UserRepository.class), new SpelAwareProxyProjectionFactory(),
				converter.getMappingContext());

		StringN1qlQueryCreator creator = new StringN1qlQueryCreator(
				getAccessor(getParameters(method), "Oliver", "Twist"), queryMethod, converter, "travel-sample",
				QueryMethodEvaluationContextProvider.DEFAULT, namedQueries);

		Query query = creator.createQuery();

		ExecutableFindByQueryOperation.ExecutableFindByQuery q = (ExecutableFindByQueryOperation.ExecutableFindByQuery) couchbaseTemplate.findByQuery(
				User.class).matching(query);

		User u = (User) q.oneValue();
		assertEquals(user, u);

		couchbaseTemplate.removeById().one(user.getId());
	}

	private ParameterAccessor getAccessor(Parameters<?, ?> params, Object... values) {
		return new ParametersParameterAccessor(params, values);
	}

	private Parameters<?, ?> getParameters(Method method) {
		return new DefaultParameters(method);
	}

}
