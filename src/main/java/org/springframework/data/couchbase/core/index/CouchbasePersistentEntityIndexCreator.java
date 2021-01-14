/*
 * Copyright 2012-2020 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.core.index;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationListener;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.index.CouchbasePersistentEntityIndexResolver.IndexDefinitionHolder;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.Document;

import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.java.Cluster;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.context.MappingContextEvent;

/**
 * Encapsulates the logic of creating indices.
 *
 * @author Michael Nitschinger
 * @author Aaron Whiteside
 */
public class CouchbasePersistentEntityIndexCreator implements InitializingBean, ApplicationListener<MappingContextEvent<?, ?>> {

	private static final Logger LOGGER = LoggerFactory.getLogger(CouchbasePersistentEntityIndexCreator.class);

	private final Map<Class<?>, Boolean> classesSeen = new ConcurrentHashMap<>();
	private final CouchbaseMappingContext mappingContext;
	private final QueryIndexResolver indexResolver;
	private final CouchbaseClientFactory clientFactory;
	private final boolean enabled;

	public CouchbasePersistentEntityIndexCreator(final CouchbaseMappingContext mappingContext,
			final CouchbaseClientFactory clientFactory, final String typeKey, final boolean enabled) {
		this.mappingContext = mappingContext;
		this.clientFactory = clientFactory;
		this.enabled = enabled;
		this.indexResolver = QueryIndexResolver.create(mappingContext, typeKey);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (enabled) {
			mappingContext.getPersistentEntities().forEach(this::checkForIndexes);
		} else {
			LOGGER.debug("Automatic index creation not enabled.");
		}
	}

	@Override
	public void onApplicationEvent(final MappingContextEvent<?, ?> event) {
		if (!enabled || !event.wasEmittedBy(mappingContext)) {
			return;
		}

		PersistentEntity<?, ?> entity = event.getPersistentEntity();

		// Double check type as Spring infrastructure does not consider nested generics
		if (entity instanceof CouchbasePersistentEntity) {
			checkForIndexes((CouchbasePersistentEntity<?>) entity);
		}
	}

	private void checkForIndexes(final CouchbasePersistentEntity<?> entity) {
		Class<?> type = entity.getType();

		if (!classesSeen.containsKey(type)) {
			this.classesSeen.put(type, Boolean.TRUE);

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Analyzing class {} for index information.", type);
			}

			checkForAndCreateIndexes(entity);
		}
	}

	private void checkForAndCreateIndexes(final CouchbasePersistentEntity<?> entity) {
		if (entity.isAnnotationPresent(Document.class)) {

			for (IndexDefinition indexDefinition : indexResolver.resolveIndexFor(entity.getTypeInformation())) {
				IndexDefinitionHolder indexToCreate = indexDefinition instanceof IndexDefinitionHolder
						? (IndexDefinitionHolder) indexDefinition
						: new IndexDefinitionHolder(indexDefinition.getIndexFields(), indexDefinition.getIndexName(),
								indexDefinition.getIndexPredicate());

				createIndex(indexToCreate);
			}
		}
	}

	private void createIndex(final IndexDefinitionHolder indexToCreate) {
		Cluster cluster = clientFactory.getCluster();

		StringBuilder statement = new StringBuilder("CREATE INDEX ").append(indexToCreate.getIndexName()).append(" ON `")
				.append(clientFactory.getBucket().name()).append("` (")
				.append(String.join(",", indexToCreate.getIndexFields())).append(")");

		if (indexToCreate.getIndexPredicate() != null && !indexToCreate.getIndexPredicate().isEmpty()) {
			statement.append(" WHERE ").append(indexToCreate.getIndexPredicate());
		}

		try {
			cluster.query(statement.toString());
		} catch (IndexExistsException ex) {
			// ignored on purpose, rest is propagated
			LOGGER.debug("Index \"{}\" already exists, ignoring.", indexToCreate.getIndexName());
		} catch (Exception ex) {
			throw new DataIntegrityViolationException("Could not auto-create index with statement: " + statement.toString(),
					ex);
		}
	}
}
