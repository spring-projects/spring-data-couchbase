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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.repository.support.MappingCouchbaseEntityInformation;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

public class CouchbasePersistentEntityIndexResolver implements QueryIndexResolver {

	private final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;
	private final String typeKey;

	public CouchbasePersistentEntityIndexResolver(
			final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext,
			final String typeKey) {
		this.mappingContext = mappingContext;
		this.typeKey = typeKey;
	}

	@Override
	public Iterable<? extends IndexDefinitionHolder> resolveIndexFor(final TypeInformation<?> typeInformation) {
		return resolveIndexForEntity(mappingContext.getRequiredPersistentEntity(typeInformation));
	}

	public List<IndexDefinitionHolder> resolveIndexForEntity(final CouchbasePersistentEntity<?> root) {
		Assert.notNull(root, "CouchbasePersistentEntity must not be null!");
		Document document = root.findAnnotation(Document.class);
		Assert.notNull(document, () -> String
				.format("Entity %s is not a collection root. Make sure to annotate it with @Document!", root.getName()));

		List<IndexDefinitionHolder> indexInformation = new ArrayList<>();

		root.doWithProperties((PropertyHandler<CouchbasePersistentProperty>) property -> this
				.potentiallyAddIndexForProperty(root, property, indexInformation));

		return indexInformation;
	}

	private void potentiallyAddIndexForProperty(final CouchbasePersistentEntity<?> root,
			final CouchbasePersistentProperty persistentProperty, final List<IndexDefinitionHolder> indexes) {
		List<IndexDefinitionHolder> indexDefinitions = createIndexDefinitionHolderForProperty(
				persistentProperty.getFieldName(), root, persistentProperty);
		if (!indexDefinitions.isEmpty()) {
			indexes.addAll(indexDefinitions);
		}
	}

	private List<IndexDefinitionHolder> createIndexDefinitionHolderForProperty(final String dotPath,
			final CouchbasePersistentEntity<?> persistentEntity, final CouchbasePersistentProperty persistentProperty) {

		List<IndexDefinitionHolder> indices = new ArrayList<>();

		if (persistentProperty.isAnnotationPresent(QueryIndexed.class)) {
			indices.add(createFieldQueryIndexDefinition(persistentEntity, persistentProperty));
		}

		if (persistentEntity.isAnnotationPresent(CompositeQueryIndex.class)
				|| persistentEntity.isAnnotationPresent(CompositeQueryIndexes.class)) {
			indices.addAll(createCompositeQueryIndexDefinitions(persistentEntity, persistentProperty));
		}

		return indices;
	}

	@Nullable
	protected IndexDefinitionHolder createFieldQueryIndexDefinition(final CouchbasePersistentEntity<?> entity,
			final CouchbasePersistentProperty property) {
		QueryIndexed index = property.findAnnotation(QueryIndexed.class);
		if (index == null) {
			return null;
		}

		MappingCouchbaseEntityInformation<?, Object> entityInfo = new MappingCouchbaseEntityInformation<>(entity);

		List<String> fields = new ArrayList<>();
		String fieldName = index.name().isEmpty() ? property.getFieldName() : index.name();
		fields.add(fieldName + (index.direction() == QueryIndexDirection.DESCENDING ? " DESC" : ""));

		String indexName = "idx_" + StringUtils.uncapitalize(entity.getType().getSimpleName()) + "_" + fieldName;

		return new IndexDefinitionHolder(fields, indexName, getPredicate(entityInfo));
	}

	protected List<IndexDefinitionHolder> createCompositeQueryIndexDefinitions(final CouchbasePersistentEntity<?> entity,
			final CouchbasePersistentProperty property) {

		List<CompositeQueryIndex> indexAnnotations = new ArrayList<>();

		if (entity.isAnnotationPresent(CompositeQueryIndex.class)) {
			indexAnnotations.add(entity.findAnnotation(CompositeQueryIndex.class));
		}
		if (entity.isAnnotationPresent(CompositeQueryIndexes.class)) {
			indexAnnotations.addAll(Arrays.asList(entity.findAnnotation(CompositeQueryIndexes.class).value()));
		}

		MappingCouchbaseEntityInformation<?, Object> entityInfo = new MappingCouchbaseEntityInformation<>(entity);

		String predicate = getPredicate(entityInfo);

		return indexAnnotations.stream().map(ann -> {
			List<String> fields = Arrays.asList(ann.fields());
			String fieldsIndexName = String.join("_", fields).toLowerCase().replace(" ", "").replace("asc", "")
					.replace("desc", "");

			String indexName = "idx_" + StringUtils.uncapitalize(entity.getType().getSimpleName()) + "_" + fieldsIndexName;
			return new IndexDefinitionHolder(fields, indexName, predicate);
		}).collect(Collectors.toList());
	}

	private String getPredicate(final MappingCouchbaseEntityInformation<?, Object> entityInfo) {
		String typeValue = entityInfo.getJavaType().getName();
		return "`" + typeKey + "` = \"" + typeValue + "\"";
	}

	public static class IndexDefinitionHolder implements IndexDefinition {

		private final List<String> fields;
		private final String indexName;
		private final String indexPredicate;

		public IndexDefinitionHolder(List<String> fields, String indexName, String indexPredicate) {
			this.fields = fields;
			this.indexName = indexName;
			this.indexPredicate = indexPredicate;
		}

		@Override
		public List<String> getIndexFields() {
			return fields;
		}

		@Override
		public String getIndexName() {
			return indexName;
		}

		@Override
		public String getIndexPredicate() {
			return indexPredicate;
		}

	}

}
