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

package org.springframework.data.couchbase.core.mapping;

import java.util.Arrays;
import java.util.UUID;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.TestContainerResource;
import org.springframework.data.couchbase.core.convert.CouchbaseCustomConversions;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.springframework.data.couchbase.CouchbaseTestHelper.getRepositoryWithRetry;

/**
 * @author Subhashni Balakrishnan
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
public class CustomConverterIntegrationTests {

	@Autowired
	private MappingCouchbaseConverter converter;

	@Autowired
	private RepositoryOperationsMapping operationsMapping;

	@Autowired
	private IndexManager indexManager;


	private TestUUIDRepository repository;

	@WritingConverter
	public enum UUIDToStringConverter implements Converter<UUID, String> {
		INSTANCE;

		@Override
		public String convert(UUID source) {
			return source.toString();
		}
	}

	@ReadingConverter
	public enum StringToUUIDConverter implements Converter<String, UUID> {
		INSTANCE;

		@Override
		public UUID convert(String source) {
			return UUID.fromString(source);
		}
	}

	@Before
	public void setup() {
		converter.setCustomConversions(new CouchbaseCustomConversions(Arrays.asList(UUIDToStringConverter.INSTANCE, StringToUUIDConverter.INSTANCE)));
		converter.afterPropertiesSet();
		RepositoryFactorySupport factory = new CouchbaseRepositoryFactory(operationsMapping, indexManager);
		repository = getRepositoryWithRetry(factory, TestUUIDRepository.class);
	}

	@Test
	public void testIdConversion() {
		TestUUID doc = new TestUUID();
		doc.id = UUID.randomUUID();
		repository.save(doc);
		repository.findById(doc.id);
	}
}
