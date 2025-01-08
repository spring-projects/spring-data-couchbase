/*
 * Copyright 2017-2025 the original author or authors.
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Method;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.StringQuery;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserRepository;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;
import org.springframework.data.repository.query.DefaultParameters;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.ParametersSource;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
class StringN1qlQueryCreatorTests {

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

	@Test
	void createsQueryCorrectly() throws Exception {
		String input = "getByFirstnameAndLastname";
		Method method = UserRepository.class.getMethod(input, String.class, String.class);

		CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method,
				new DefaultRepositoryMetadata(UserRepository.class), new SpelAwareProxyProjectionFactory(),
				converter.getMappingContext());

		StringN1qlQueryCreator creator = new StringN1qlQueryCreator(getAccessor(getParameters(method), "Oliver", "Twist"),
				queryMethod, converter, new SpelExpressionParser(), QueryMethodEvaluationContextProvider.DEFAULT, namedQueries);

		Query query = creator.createQuery();
		assertEquals(
				"SELECT `_class`, `jsonNode`, `jsonObject`, `jsonArray`, META(`" + bucketName()
						+ "`).`cas` AS __cas, `createdBy`, `createdDate`, `lastModifiedBy`, `lastModifiedDate`, META(`"
						+ bucketName() + "`).`id` AS __id, `firstname`, `lastname`, `subtype` FROM `" + bucketName()
						+ "` where `_class` = \"abstractuser\" and firstname = $1 and lastname = $2",
				query.toN1qlSelectString(converter, bucketName(), null, null, User.class, User.class, false, null, null));
	}

	@Test
	void createsQueryCorrectly2() throws Exception {
		String input = "getByFirstnameOrLastname";
		Method method = UserRepository.class.getMethod(input, String.class, String.class);

		CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method,
				new DefaultRepositoryMetadata(UserRepository.class), new SpelAwareProxyProjectionFactory(),
				converter.getMappingContext());

		StringN1qlQueryCreator creator = new StringN1qlQueryCreator(getAccessor(getParameters(method), "Oliver", "Twist"),
				queryMethod, converter, new SpelExpressionParser(), QueryMethodEvaluationContextProvider.DEFAULT, namedQueries);

		Query query = creator.createQuery();
		assertEquals(
				"SELECT `_class`, `jsonNode`, `jsonObject`, `jsonArray`, META(`" + bucketName()
						+ "`).`cas` AS __cas, `createdBy`, `createdDate`, `lastModifiedBy`, `lastModifiedDate`, META(`"
						+ bucketName() + "`).`id` AS __id, `firstname`, `lastname`, `subtype` FROM `" + bucketName()
						+ "` where `_class` = \"abstractuser\" and (firstname = $first or lastname = $last)",
				query.toN1qlSelectString(converter, bucketName(), null, null, User.class, User.class, false, null, null));
	}

	@Test
	void stringQuerycreatesQueryCorrectly() throws Exception {
		String queryString = "a b c";
		Query query = new StringQuery(queryString);
		assertEquals(queryString, query.toN1qlSelectString(converter, bucketName(), null, null, User.class, User.class,
				false, null, null));
	}

	@Test
	void stringQueryNoPositionalParameters() {
		String queryString = " $1";
		Query query = new StringQuery(queryString);
		assertThrows(IllegalArgumentException.class, () -> query.toN1qlSelectString(converter, bucketName(), null, null,
				User.class, User.class, false, null, null));
	}

	@Test
	void stringQueryNoNamedParameters() {
		String queryString = " $george";
		Query query = new StringQuery(queryString);
		assertThrows(IllegalArgumentException.class, () -> query.toN1qlSelectString(converter, bucketName(), null, null,
				User.class, User.class, false, null, null));
	}

	@Test
	void stringQueryNoSpelExpressions() {
		String queryString = "#{#n1ql.filter}";
		Query query = new StringQuery(queryString);
		assertThrows(IllegalArgumentException.class, () -> query.toN1qlSelectString(converter, bucketName(), null, null,
				User.class, User.class, false, null, null));
	}

	@Test
	void stringQueryNoPositionalParametersQuotes() {
		String queryString = " '$1'";
		Query query = new StringQuery(queryString);
		query.toN1qlSelectString(converter, bucketName(), null, null, User.class, User.class, false, null, null);
	}

	@Test
	void stringQueryNoNamedParametersQuotes() {
		String queryString = " '$george'";
		Query query = new StringQuery(queryString);
		query.toN1qlSelectString(converter, bucketName(), null, null, User.class, User.class, false, null, null);
	}

	@Test
	void stringQueryNoSpelExpressionsQuotes() {
		String queryString = "'#{#n1ql.filter}'";
		Query query = new StringQuery(queryString);
		query.toN1qlSelectString(converter, bucketName(), null, null, User.class, User.class, false, null, null);
	}

	@Test
	void spelTests() throws Exception {
		String input = "spelTests";
		Method method = UserRepository.class.getMethod(input);
		CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method,
				new DefaultRepositoryMetadata(UserRepository.class), new SpelAwareProxyProjectionFactory(),
				converter.getMappingContext());

		StringN1qlQueryCreator creator = new StringN1qlQueryCreator(getAccessor(getParameters(method)), queryMethod,
				converter, new SpelExpressionParser(), QueryMethodEvaluationContextProvider.DEFAULT, namedQueries);

		Query query = creator.createQuery();

		assertEquals("SELECT `_class`, `jsonNode`, `jsonObject`, `jsonArray`, META(`myCollection`).`cas`"
				+ " AS __cas, `createdBy`, `createdDate`, `lastModifiedBy`, `lastModifiedDate`, META(`myCollection`).`id`"
				+ " AS __id, `firstname`, `lastname`, `subtype` FROM `myCollection`|`_class` = \"abstractuser\"|`some_bucket`|`myScope`|`myCollection`",
				query.toN1qlSelectString(converter, bucketName(), "myScope", "myCollection", User.class, null, false, null,
						null));
	}

	private String bucketName() {
		return "some_bucket";
	}

	private ParameterAccessor getAccessor(Parameters<?, ?> params, Object... values) {
		return new ParametersParameterAccessor(params, values);
	}

	private Parameters<?, ?> getParameters(Method method) {
		return new DefaultParameters(ParametersSource.of(method));
	}

}
