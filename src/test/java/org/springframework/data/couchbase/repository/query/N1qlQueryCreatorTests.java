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
import org.springframework.data.repository.query.DefaultParameters;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.parser.PartTree;

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

		N1qlQueryCreator creator = new N1qlQueryCreator(tree, getAccessor(getParameters(method), "Oliver"), context);
		Query query = creator.createQuery();

		assertEquals(query.export(), " WHERE " + where("firstname").is("Oliver").export());
	}

	@Test
	void createsAndQueryCorrectly() throws Exception {
		String input = "findByFirstnameAndLastname";
		PartTree tree = new PartTree(input, User.class);
		Method method = UserRepository.class.getMethod(input, String.class, String.class);

		N1qlQueryCreator creator = new N1qlQueryCreator(tree, getAccessor(getParameters(method), "John", "Doe"), context);
		Query query = creator.createQuery();

		assertEquals(query.export(), " WHERE " + where("firstname").is("John").and("lastname").is("Doe").export());
	}

	private ParameterAccessor getAccessor(Parameters<?, ?> params, Object... values) {
		return new ParametersParameterAccessor(params, values);
	}

	private Parameters<?, ?> getParameters(Method method) {
		return new DefaultParameters(method);
	}

}
