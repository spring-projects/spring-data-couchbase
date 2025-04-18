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
import static org.springframework.data.couchbase.core.query.N1QLExpression.i;
import static org.springframework.data.couchbase.core.query.N1QLExpression.x;
import static org.springframework.data.couchbase.core.query.QueryCriteria.where;

import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.java.query.QueryOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonRepository;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserRepository;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.DefaultParameters;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.ParametersSource;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.PartTree;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Mauro Monti
 */
class N1qlQueryCreatorTests {

	MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> context;
	CouchbaseConverter converter;
	String bucketName;

	@BeforeEach
	public void beforeEach() {
		context = new CouchbaseMappingContext();
		converter = new MappingCouchbaseConverter(context);
		bucketName = "sample-bucket";
	}

	@Test
	void createsQueryCorrectly() throws Exception {
		String input = "findByFirstname";
		PartTree tree = new PartTree(input, User.class);
		Method method = UserRepository.class.getMethod(input, String.class);
		QueryMethod queryMethod = new QueryMethod(method, new DefaultRepositoryMetadata(UserRepository.class),
				new SpelAwareProxyProjectionFactory());
		N1qlQueryCreator creator = new N1qlQueryCreator(tree, getAccessor(getParameters(method), "Oliver"), queryMethod,
				converter, bucketName);
		Query query = creator.createQuery();

		assertEquals(query.export(), " WHERE " + where(i("firstname")).is("Oliver").export());
	}

	@Test
	void createsQueryCorrectlyIgnoreCase() throws Exception {
		String input = "findByFirstnameIgnoreCase";
		PartTree tree = new PartTree(input, User.class);
		Method method = UserRepository.class.getMethod(input, String.class);
		QueryMethod queryMethod = new QueryMethod(method, new DefaultRepositoryMetadata(UserRepository.class),
				new SpelAwareProxyProjectionFactory());
		N1qlQueryCreator creator = new N1qlQueryCreator(tree, getAccessor(getParameters(method), "Oliver"), queryMethod,
				converter, bucketName);
		Query query = creator.createQuery();

		assertEquals(query.export(),
				" WHERE " + where("lower(" + i("firstname") + ")").is("Oliver".toLowerCase(Locale.ROOT)).export());
	}

	@Test
	void createsQueryFieldAnnotationCorrectly() throws Exception {
		String input = "findByMiddlename";
		PartTree tree = new PartTree(input, Person.class);
		Method method = PersonRepository.class.getMethod(input, String.class);
		QueryMethod queryMethod = new QueryMethod(method, new DefaultRepositoryMetadata(UserRepository.class),
				new SpelAwareProxyProjectionFactory());
		N1qlQueryCreator creator = new N1qlQueryCreator(tree, getAccessor(getParameters(method), "Oliver"), queryMethod,
				converter, bucketName);
		Query query = creator.createQuery();

		assertEquals(query.export(), " WHERE " + where(i("nickname")).is("Oliver").export());
	}

	@Test
	void queryParametersArray() throws Exception {
		String input = "findByFirstnameIn";
		PartTree tree = new PartTree(input, User.class);
		Method method = UserRepository.class.getMethod(input, String[].class);
		QueryMethod queryMethod = new QueryMethod(method, new DefaultRepositoryMetadata(UserRepository.class),
				new SpelAwareProxyProjectionFactory());
		Query expected = (new Query()).addCriteria(where(i("firstname")).in("Oliver", "Charles"));
		JsonArray parameters = JsonArray.create().add(JsonArray.create().add("Oliver").add("Charles"));
		QueryOptions expectedQOptions = QueryOptions.queryOptions().parameters(parameters);
		N1qlQueryCreator creator = new N1qlQueryCreator(tree,
				getAccessor(getParameters(method), new Object[] { new Object[] { "Oliver", "Charles" } }), queryMethod,
				converter, bucketName);
		Query query = creator.createQuery();

		assertEquals(" WHERE `firstname` in $1", query.export(new int[1]));
		ArrayNode expectedOptions = expected.buildQueryOptions(expectedQOptions, null).build().positionalParameters();
		ArrayNode actualOptions = query.buildQueryOptions(null, null).build().positionalParameters();
		assertEquals(expectedOptions.toString(), actualOptions.toString());
	}

	@Test
	void queryParametersJsonArray() throws Exception {
		String input = "findByFirstnameIn";
		PartTree tree = new PartTree(input, User.class);
		Method method = UserRepository.class.getMethod(input, JsonArray.class);
		QueryMethod queryMethod = new QueryMethod(method, new DefaultRepositoryMetadata(UserRepository.class),
				new SpelAwareProxyProjectionFactory());
		JsonArray jsonArray = JsonArray.create();
		jsonArray.add("Oliver");
		jsonArray.add("Charles");
		N1qlQueryCreator creator = new N1qlQueryCreator(tree, getAccessor(getParameters(method), jsonArray), queryMethod,
				converter, bucketName);
		Query query = creator.createQuery();

		Query expected = (new Query()).addCriteria(where(i("firstname")).in("Oliver", "Charles"));
		JsonArray parameters = JsonArray.create().add(JsonArray.create().add("Oliver").add("Charles"));
		QueryOptions expectedQOptions = QueryOptions.queryOptions().parameters(parameters);
		assertEquals(" WHERE `firstname` in $1", query.export(new int[1]));
		ArrayNode expectedOptions = expected.buildQueryOptions(expectedQOptions, null).build().positionalParameters();
		ArrayNode actualOptions = query.buildQueryOptions(null, null).build().positionalParameters();
		assertEquals(expectedOptions.toString(), actualOptions.toString());
	}

	@Test
	void queryParametersList() throws Exception {
		String input = "findByFirstnameIn";
		PartTree tree = new PartTree(input, User.class);
		Method method = UserRepository.class.getMethod(input, String[].class);
		QueryMethod queryMethod = new QueryMethod(method, new DefaultRepositoryMetadata(UserRepository.class),
				new SpelAwareProxyProjectionFactory());
		List<String> list = new LinkedList<>();
		list.add("Oliver");
		list.add("Charles");
		N1qlQueryCreator creator = new N1qlQueryCreator(tree, getAccessor(getParameters(method), new Object[] { list }),
				queryMethod, converter, bucketName);
		Query query = creator.createQuery();

		Query expected = (new Query()).addCriteria(where(i("firstname")).in("Oliver", "Charles"));
		JsonArray parameters = JsonArray.create().add(JsonArray.create().add("Oliver").add("Charles"));
		QueryOptions expectedQOptions = QueryOptions.queryOptions().parameters(parameters);

		assertEquals(" WHERE `firstname` in $1", query.export(new int[1]));
		ArrayNode expectedOptions = expected.buildQueryOptions(expectedQOptions, null).build().positionalParameters();
		ArrayNode actualOptions = query.buildQueryOptions(null, null).build().positionalParameters();
		assertEquals(expectedOptions.toString(), actualOptions.toString());
	}

	@Test
	void createsAndQueryCorrectly() throws Exception {
		String input = "findByFirstnameAndLastname";
		PartTree tree = new PartTree(input, User.class);
		Method method = UserRepository.class.getMethod(input, String.class, String.class);
		QueryMethod queryMethod = new QueryMethod(method, new DefaultRepositoryMetadata(UserRepository.class),
				new SpelAwareProxyProjectionFactory());
		N1qlQueryCreator creator = new N1qlQueryCreator(tree, getAccessor(getParameters(method), "John", "Doe"),
				queryMethod, converter, bucketName);
		Query query = creator.createQuery();

		assertEquals(" WHERE " + where(i("firstname")).is("John").and(i("lastname")).is("Doe").export(), query.export());
	}

	@Test // https://github.com/spring-projects/spring-data-couchbase/issues/1072
	void createsQueryFindByIdIsNotNullAndFirstname() throws Exception {
		String input = "findByIdIsNotNullAndFirstnameEquals";
		PartTree tree = new PartTree(input, User.class);
		Method method = UserRepository.class.getMethod(input, String.class);
		QueryMethod queryMethod = new QueryMethod(method, new DefaultRepositoryMetadata(UserRepository.class),
				new SpelAwareProxyProjectionFactory());
		N1qlQueryCreator creator = new N1qlQueryCreator(tree, getAccessor(getParameters(method), "Oliver"), queryMethod,
				converter, bucketName);
		Query query = creator.createQuery();

		assertEquals(" WHERE " + where(x("META().`id`")).isNotNull().and(i("firstname")).is("Oliver").export(),
				query.export());
	}

	@Test // https://github.com/spring-projects/spring-data-couchbase/issues/1072
	void createsQueryFindByVersionEqualsAndAndFirstname() throws Exception {
		String input = "findByVersionEqualsAndFirstnameEquals";
		PartTree tree = new PartTree(input, User.class);
		Method method = UserRepository.class.getMethod(input, Long.class, String.class);
		QueryMethod queryMethod = new QueryMethod(method, new DefaultRepositoryMetadata(UserRepository.class),
				new SpelAwareProxyProjectionFactory());
		N1qlQueryCreator creator = new N1qlQueryCreator(tree,
				getAccessor(getParameters(method), 1611287177404088320L, "Oliver"), queryMethod, converter, bucketName);
		Query query = creator.createQuery();

		assertEquals(
				" WHERE " + where(x("META().`cas`")).is(1611287177404088320L).and(i("firstname")).is("Oliver").export(),
				query.export());
	}

	private ParameterAccessor getAccessor(Parameters<?, ?> params, Object... values) {
		return new ParametersParameterAccessor(params, values);
	}

	private Parameters<?, ?> getParameters(Method method) {
		return new DefaultParameters(ParametersSource.of(method));
	}

}
