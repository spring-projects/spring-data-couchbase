/*
 * Copyright 2025-present the original author or authors
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
import org.springframework.context.ApplicationListener;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.repository.SearchIndex;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.MappingContextEvent;

import com.couchbase.client.java.Cluster;

/**
 * Automatically creates FTS (Full-Text Search) indexes for entity classes annotated with {@link SearchIndex}.
 * <p>
 * This creator listens for {@link MappingContextEvent}s and, when an entity annotated with {@code @SearchIndex}
 * is discovered, creates a default FTS index on the bucket if one does not already exist.
 * <p>
 * The created index uses the default mapping (all fields indexed) and is named after the annotation value.
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
public class CouchbasePersistentEntitySearchIndexCreator implements ApplicationListener<MappingContextEvent<?, ?>> {

	private static final Logger LOGGER = LoggerFactory.getLogger(CouchbasePersistentEntitySearchIndexCreator.class);

	private final Map<String, Boolean> indexesSeen = new ConcurrentHashMap<>();
	private final CouchbaseMappingContext mappingContext;
	private final CouchbaseOperations couchbaseOperations;

	public CouchbasePersistentEntitySearchIndexCreator(final CouchbaseMappingContext mappingContext,
			final CouchbaseOperations operations) {
		this.mappingContext = mappingContext;
		this.couchbaseOperations = operations;
	}

	@Override
	public void onApplicationEvent(final MappingContextEvent<?, ?> event) {
		if (!event.wasEmittedBy(mappingContext)) {
			return;
		}

		PersistentEntity<?, ?> entity = event.getPersistentEntity();

		if (entity instanceof CouchbasePersistentEntity) {
			checkForSearchIndex((CouchbasePersistentEntity<?>) entity);
		}
	}

	private void checkForSearchIndex(final CouchbasePersistentEntity<?> entity) {
		SearchIndex searchIndexAnnotation = entity.findAnnotation(SearchIndex.class);
		if (searchIndexAnnotation == null) {
			return;
		}

		String indexName = searchIndexAnnotation.value();
		if (indexesSeen.containsKey(indexName)) {
			return;
		}

		indexesSeen.put(indexName, Boolean.TRUE);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Ensuring FTS index '{}' exists for entity {}", indexName, entity.getType().getSimpleName());
		}

		createSearchIndexIfNotExists(indexName);
	}

	private void createSearchIndexIfNotExists(String indexName) {
		Cluster cluster = couchbaseOperations.getCouchbaseClientFactory().getCluster();
		String bucketName = couchbaseOperations.getBucketName();

		try {
			cluster.searchIndexes().getIndex(indexName);
			LOGGER.debug("FTS index '{}' already exists, skipping creation.", indexName);
		} catch (com.couchbase.client.core.error.IndexNotFoundException ex) {
			try {
				com.couchbase.client.java.manager.search.SearchIndex index =
						new com.couchbase.client.java.manager.search.SearchIndex(indexName, bucketName);
				cluster.searchIndexes().upsertIndex(index);
				LOGGER.info("Created FTS index '{}' on bucket '{}'", indexName, bucketName);
			} catch (Exception createEx) {
				LOGGER.warn("Failed to auto-create FTS index '{}': {}", indexName, createEx.getMessage());
			}
		} catch (Exception ex) {
			LOGGER.warn("Failed to check FTS index '{}': {}", indexName, ex.getMessage());
		}
	}

	public boolean isIndexCreatorFor(final MappingContext<?, ?> context) {
		return this.mappingContext.equals(context);
	}
}
