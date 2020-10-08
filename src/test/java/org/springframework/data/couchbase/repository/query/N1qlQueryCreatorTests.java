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
import static org.springframework.data.couchbase.core.query.QueryCriteria.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserRepository;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.query.*;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
class N1qlQueryCreatorTests {

	MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> context;
	CouchbaseConverter converter;

	@BeforeEach
	public void beforeEach() {
		context = new CouchbaseMappingContext();
		converter = new MappingCouchbaseConverter(context);
	}

	@Test
	void createsQueryCorrectly() throws Exception {
		String input = "findByFirstname";
		PartTree tree = new PartTree(input, User.class);
		Method method = UserRepository.class.getMethod(input, String.class);

		N1qlQueryCreator creator = new N1qlQueryCreator(tree, getAccessor(getParameters(method), "Oliver"), null,
				converter);
		Query query = creator.createQuery();

		assertEquals(query.export(), " WHERE " + where("firstname").is("Oliver").export());
	}

	@Test
	void queryParametersArray() throws Exception {
		String input = "findByFirstnameIn";
		PartTree tree = new PartTree(input, User.class);
		Method method = UserRepository.class.getMethod(input, String[].class);
		Query expected = (new Query()).addCriteria(where("firstname").in("Oliver", "Charles"));
		N1qlQueryCreator creator = new N1qlQueryCreator(tree,
				getAccessor(getParameters(method), new Object[] { new Object[] { "Oliver", "Charles" } }), null, converter);
		Query query = creator.createQuery();

		// Query expected = (new Query()).addCriteria(where("firstname").in("Oliver", "Charles"));
		assertEquals(expected.export(new int[1]), query.export(new int[1]));
		JsonObject expectedOptions = JsonObject.create();
		expected.buildQueryOptions(null).build().injectParams(expectedOptions);
		JsonObject actualOptions = JsonObject.create();
		expected.buildQueryOptions(null).build().injectParams(actualOptions);
		assertEquals(expectedOptions.removeKey("client_context_id"), actualOptions.removeKey("client_context_id"));
	}

	@Test
	void queryParametersJsonArray() throws Exception {
		String input = "findByFirstnameIn";
		PartTree tree = new PartTree(input, User.class);
		Method method = UserRepository.class.getMethod(input, JsonArray.class);

		JsonArray jsonArray = JsonArray.create();
		jsonArray.add("Oliver");
		jsonArray.add("Charles");
		N1qlQueryCreator creator = new N1qlQueryCreator(tree, getAccessor(getParameters(method), jsonArray), null,
				converter);
		Query query = creator.createQuery();

		Query expected = (new Query()).addCriteria(where("firstname").in("Oliver", "Charles"));
		assertEquals(expected.export(new int[1]), query.export(new int[1]));
		JsonObject expectedOptions = JsonObject.create();
		expected.buildQueryOptions(null).build().injectParams(expectedOptions);
		JsonObject actualOptions = JsonObject.create();
		expected.buildQueryOptions(null).build().injectParams(actualOptions);
		assertEquals(expectedOptions.removeKey("client_context_id"), actualOptions.removeKey("client_context_id"));
	}

	@Test
	void queryParametersList() throws Exception {
		String input = "findByFirstnameIn";
		PartTree tree = new PartTree(input, User.class);
		Method method = UserRepository.class.getMethod(input, String[].class);
		List<String> list = new LinkedList<>();
		list.add("Oliver");
		list.add("Charles");
		N1qlQueryCreator creator = new N1qlQueryCreator(tree, getAccessor(getParameters(method), new Object[] { list }),
				null, converter);
		Query query = creator.createQuery();

		Query expected = (new Query()).addCriteria(where("firstname").in("Oliver", "Charles"));

		assertEquals(expected.export(new int[1]), query.export(new int[1]));
		JsonObject expectedOptions = JsonObject.create();
		expected.buildQueryOptions(null).build().injectParams(expectedOptions);
		JsonObject actualOptions = JsonObject.create();
		expected.buildQueryOptions(null).build().injectParams(actualOptions);
		assertEquals(expectedOptions.removeKey("client_context_id"), actualOptions.removeKey("client_context_id"));
	}

	@Test
	void createsAndQueryCorrectly() throws Exception {
		String input = "findByFirstnameAndLastname";
		PartTree tree = new PartTree(input, User.class);
		Method method = UserRepository.class.getMethod(input, String.class, String.class);
		N1qlQueryCreator creator = new N1qlQueryCreator(tree, getAccessor(getParameters(method), "John", "Doe"), null,
				converter);
		Query query = creator.createQuery();

		assertEquals(" WHERE " + where("firstname").is("John").and("lastname").is("Doe").export(), query.export());
	}

	private ParameterAccessor getAccessor(Parameters<?, ?> params, Object... values) {
		return new ParametersParameterAccessor(params, values);
	}

	private Parameters<?, ?> getParameters(Method method) {
		return new DefaultParameters(method);
	}

}
