/*
 * Copyright 2011-2022 the original author or authors.
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
package org.springframework.data.couchbase.repository.support;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import com.querydsl.couchbase.document.CouchbaseDocumentSerializer;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.QueryCriteriaDefinition;
import org.springframework.data.mapping.context.MappingContext;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.querydsl.core.types.Constant;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Operation;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.PathMetadata;
import com.querydsl.core.types.PathType;

/**
 * Custom {@link CouchbaseDocumentSerializer} to take mapping information into account when building keys for
 * constraints.
 *
 * @author Michael Reiche
 */
public class SpringDataCouchbaseSerializer extends CouchbaseDocumentSerializer {

	private static final String ID_KEY = "_id";
	private static final Set<PathType> PATH_TYPES;

	static {

		Set<PathType> pathTypes = new HashSet<>();
		pathTypes.add(PathType.VARIABLE);
		pathTypes.add(PathType.PROPERTY);

		PATH_TYPES = Collections.unmodifiableSet(pathTypes);
	}

	private final CouchbaseConverter converter;
	private final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;
	// private final QueryMapper mapper;

	/**
	 * Creates a new {@link SpringDataCouchbaseSerializer} for the given {@link CouchbaseConverter}.
	 *
	 * @param converter must not be {@literal null}.
	 */
	public SpringDataCouchbaseSerializer(CouchbaseConverter converter) {

		Assert.notNull(converter, "CouchbaseConverter must not be null!");

		this.mappingContext = converter.getMappingContext();
		this.converter = converter;
		// this.mapper = new QueryMapper(converter);
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.couchbase.CouchbaseSerializer#visit(com.querydsl.core.types.Constant, java.lang.Void)
	 */
	@Override
	public Object visit(Constant<?> expr, Void context) {

		if (!ClassUtils.isAssignable(Enum.class, expr.getType())) {
			return super.visit(expr, context);
		}

		return converter.convertForWriteIfNeeded(expr.getConstant());
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.couchbase.CouchbaseSerializer#getKeyForPath(com.querydsl.core.types.Path, com.querydsl.core.types.PathMetadata)
	 */
	@Override
	protected String getKeyForPath(Path<?> expr, PathMetadata metadata) {
		// TODO - substitutions for meta().id, meta().expiry, meta().cas
		if (!metadata.getPathType().equals(PathType.PROPERTY)) {
			return super.getKeyForPath(expr, metadata);
		}

		Path<?> parent = metadata.getParent();
		CouchbasePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(parent.getType());
		CouchbasePersistentProperty property = entity.getPersistentProperty(metadata.getName());

		return property == null ? super.getKeyForPath(expr, metadata) : property.getFieldName();
	}

	/*
	 * (non-Javadoc)
	 * @see  org.springframework.data.couchbase.repository.support.CouchbaseSerializer#asDocument(java.lang.String, java.lang.Object)
	 */
	@Override
	protected QueryCriteriaDefinition asDocument(@Nullable String key, @Nullable Object value) {

		value = value instanceof Optional ? ((Optional) value).orElse(null) : value;

		return super.asDocument(key, value instanceof Pattern ? value : converter.convertForWriteIfNeeded(value));
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.couchbase.CouchbaseSerializer#isReference(com.querydsl.core.types.Path)
	 */
	@Override
	protected boolean isReference(@Nullable Path<?> path) {

		CouchbasePersistentProperty property = getPropertyForPotentialDbRef(path);
		return property == null ? false : property.isAssociation();
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.couchbase.CouchbaseSerializer#asReference(java.lang.Object)
	 */
	@Override
	protected DBRef asReference(@Nullable Object constant) {
		return asReference(constant, null);
	}

	protected DBRef asReference(Object constant, Path<?> path) {
		return null; // converter.toDBRef(constant, getPropertyForPotentialDbRef(path));
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.couchbase.CouchbaseSerializer#asDBKey(com.querydsl.core.types.Operation, int)
	 */
	@Override
	protected String asDBKey(@Nullable Operation<?> expr, int index) {

		Expression<?> arg = expr.getArg(index);
		String key = super.asDBKey(expr, index);

		if (!(arg instanceof Path)) {
			return key;
		}

		Path<?> path = (Path<?>) arg;

		if (!isReference(path)) {
			return key;
		}

		CouchbasePersistentProperty property = getPropertyFor(path);

		return property.isIdProperty() ? key.replaceAll("." + ID_KEY + "$", "") : key;
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.couchbase.CouchbaseSerializer#convert(com.querydsl.core.types.Path, com.querydsl.core.types.Constant)
	 */
	protected Object convert(@Nullable Path<?> path, @Nullable Constant<?> constant) {

		if (!isReference(path)) {
			return null;
			// return super.convert(path, constant);
		}

		CouchbasePersistentProperty property = getPropertyFor(path);

		return property.isIdProperty() ? asReference(constant.getConstant(), path.getMetadata().getParent())
				: asReference(constant.getConstant(), path);
	}

	@Nullable
	private CouchbasePersistentProperty getPropertyFor(Path<?> path) {

		Path<?> parent = path.getMetadata().getParent();

		if (parent == null || !PATH_TYPES.contains(path.getMetadata().getPathType())) {
			return null;
		}

		CouchbasePersistentEntity<?> entity = mappingContext.getPersistentEntity(parent.getType());
		return entity != null ? entity.getPersistentProperty(path.getMetadata().getName()) : null;
	}

	/**
	 * Checks the given {@literal path} for referencing the {@literal id} property of a {@link DBRef} referenced object.
	 * If so it returns the referenced {@link CouchbasePersistentProperty} of the {@link DBRef} instead of the
	 * {@literal id} property.
	 *
	 * @param path
	 * @return
	 */
	private CouchbasePersistentProperty getPropertyForPotentialDbRef(Path<?> path) {

		if (path == null) {
			return null;
		}

		CouchbasePersistentProperty property = getPropertyFor(path);
		PathMetadata metadata = path.getMetadata();

		if (property != null && property.isIdProperty() && metadata != null && metadata.getParent() != null) {
			return getPropertyFor(metadata.getParent());
		}

		return property;
	}
}
