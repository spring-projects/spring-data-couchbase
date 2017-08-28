/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository.config;

import static org.assertj.core.api.Assertions.*;

import java.io.Serializable;

import org.junit.Test;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

/**
 * Unit tests for {@link CouchbaseRepositoryConfigurationExtension}.
 *
 * @author Oliver Gierke
 */
public class CouchbaseRepositoryConfigurationExtensionUnitTests {

	@Test // DATACOUCH-330
	public void atDocumentIsIdentifyingAnnotation() {
		
		CustomCouchbaseRepositoryConfigurationExtension extension = new CustomCouchbaseRepositoryConfigurationExtension();
		RepositoryMetadata metadata = DefaultRepositoryMetadata.getMetadata(ByAggregateRootAnnotationRepository.class);
		
		assertThat(extension.isStrictRepositoryCandidate(metadata)).isTrue();
	}
	
	@Test // DATACOUCH-330
	public void couchbaseRepositoryIsIdentifyingAnnotation() {
		
		CustomCouchbaseRepositoryConfigurationExtension extension = new CustomCouchbaseRepositoryConfigurationExtension();
		RepositoryMetadata metadata = DefaultRepositoryMetadata.getMetadata(ByRepository.class);
		
		assertThat(extension.isStrictRepositoryCandidate(metadata)).isTrue();
	}
	
	@Document
	static class AggregateRoot {}
	
	interface ByAggregateRootAnnotationRepository extends Repository<AggregateRoot, Serializable> {}
	
	interface ByRepository extends CouchbaseRepository<Object, Serializable> {};
	
	static class CustomCouchbaseRepositoryConfigurationExtension extends CouchbaseRepositoryConfigurationExtension {
		
		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport#isStrictRepositoryCandidate(org.springframework.data.repository.core.RepositoryMetadata)
		 */
		@Override
		protected boolean isStrictRepositoryCandidate(RepositoryMetadata metadata) {
			return super.isStrictRepositoryCandidate(metadata);
		}
	}
}
