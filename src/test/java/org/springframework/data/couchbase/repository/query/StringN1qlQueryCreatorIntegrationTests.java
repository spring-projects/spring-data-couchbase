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

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ExecutableFindByQueryOperation.ExecutableFindByQuery;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.domain.Airline;
import org.springframework.data.couchbase.domain.Config;
import org.springframework.data.couchbase.domain.AirlineRepository;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
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
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@SpringJUnitConfig(Config.class)
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
@DirtiesContext
class StringN1qlQueryCreatorIntegrationTests extends ClusterAwareIntegrationTests {

	@Autowired MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> context;
	@Autowired CouchbaseConverter converter;
	@Autowired CouchbaseTemplate couchbaseTemplate;
	static NamedQueries namedQueries = new PropertiesBasedNamedQueries(new Properties());

	@Test
	@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
	void findUsingStringNq1l() throws Exception {
		Airline airline = new Airline(UUID.randomUUID().toString(), "Continental", "USA");
		try {
			Airline modified = couchbaseTemplate.upsertById(Airline.class).one(airline);

			String input = "getByName";
			Method method = AirlineRepository.class.getMethod(input, String.class);

			CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method,
					new DefaultRepositoryMetadata(AirlineRepository.class), new SpelAwareProxyProjectionFactory(),
					converter.getMappingContext());

			StringN1qlQueryCreator creator = new StringN1qlQueryCreator(getAccessor(getParameters(method), "Continental"),
					queryMethod, converter, new SpelExpressionParser(), QueryMethodEvaluationContextProvider.DEFAULT,
					namedQueries);

			Query query = creator.createQuery();

			ExecutableFindByQuery q = (ExecutableFindByQuery) couchbaseTemplate.findByQuery(Airline.class)
					.withConsistency(QueryScanConsistency.REQUEST_PLUS).matching(query);

			Optional<Airline> al = q.one();
			assertEquals(airline.toString(), al.get().toString());
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			couchbaseTemplate.removeById().one(airline.getId());
		}
	}

	@Test
	@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
	void findUsingStringNq1l_3x_projection_id_cas() throws Exception {
		Airline airline = new Airline(UUID.randomUUID().toString(), "Continental", "USA");
		try {
			Airline modified = couchbaseTemplate.upsertById(Airline.class).one(airline);

			String input = "getByName_3x";
			Method method = AirlineRepository.class.getMethod(input, String.class);

			CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method,
					new DefaultRepositoryMetadata(AirlineRepository.class), new SpelAwareProxyProjectionFactory(),
					converter.getMappingContext());

			StringN1qlQueryCreator creator = new StringN1qlQueryCreator(getAccessor(getParameters(method), "Continental"),
					queryMethod, converter, new SpelExpressionParser(), QueryMethodEvaluationContextProvider.DEFAULT,
					namedQueries);

			Query query = creator.createQuery();

			ExecutableFindByQuery q = (ExecutableFindByQuery) couchbaseTemplate.findByQuery(Airline.class)
					.withConsistency(QueryScanConsistency.REQUEST_PLUS).matching(query);

			Optional<Airline> al = q.one();
			assertEquals(airline.toString(), al.get().toString());
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			couchbaseTemplate.removeById().one(airline.getId());
		}
	}

	private ParameterAccessor getAccessor(Parameters<?, ?> params, Object... values) {
		return new ParametersParameterAccessor(params, values);
	}

	private Parameters<?, ?> getParameters(Method method) {
		return new DefaultParameters(method);
	}

    // static class Config extends org.springframework.data.couchbase.domain.Config {
    //		@Override
    //		protected boolean autoIndexCreation() {
    //			return true;
    //		}
    //	}
}
