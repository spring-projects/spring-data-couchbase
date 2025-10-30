/*
 * Copyright 2020-2025 the original author or authors.
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

import java.lang.reflect.Method;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.core.ReactiveWrappers;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.util.ClassUtils;

/**
 * Reactive specific implementation of {@link CouchbaseQueryMethod}.
 *
 * @author Michael Reiche
 * @since 4.1
 */
public class ReactiveCouchbaseQueryMethod extends CouchbaseQueryMethod {

	private static final TypeInformation<Page> PAGE_TYPE = TypeInformation.of(Page.class);
	private static final TypeInformation<Slice> SLICE_TYPE = TypeInformation.of(Slice.class);

	private final Lazy<Boolean> isCollectionQueryCouchbase; // not to be confused with QueryMethod.isCollectionQuery

	/**
	 * Creates a new {@link ReactiveCouchbaseQueryMethod} from the given {@link Method}.
	 *
	 * @param method must not be {@literal null}.
	 * @param metadata must not be {@literal null}.
	 * @param projectionFactory must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	public ReactiveCouchbaseQueryMethod(Method method, RepositoryMetadata metadata, ProjectionFactory projectionFactory,
			MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext) {

		super(method, metadata, projectionFactory, mappingContext);

		if (ReflectionUtils.hasParameterOfType(method, Pageable.class)) {

			TypeInformation<?> returnType = TypeInformation.fromReturnTypeOf(method);

			boolean multiWrapper = ReactiveWrappers.isMultiValueType(returnType.getType());
			boolean singleWrapperWithWrappedPageableResult = ReactiveWrappers.isSingleValueType(returnType.getType())
					&& (PAGE_TYPE.isAssignableFrom(returnType.getRequiredComponentType())
							|| SLICE_TYPE.isAssignableFrom(returnType.getRequiredComponentType()));

			if (singleWrapperWithWrappedPageableResult) {
				throw new InvalidDataAccessApiUsageException(
						String.format("'%s.%s' must not use sliced or paged execution. Please use Flux.buffer(size, skip).",
								ClassUtils.getShortName(method.getDeclaringClass()), method.getName()));
			}

			if (!multiWrapper) {
				throw new IllegalStateException(String.format(
						"Method has to use a either multi-item reactive wrapper return type or a wrapped Page/Slice type. Offending method: %s",
						method.toString()));
			}

			if (ReflectionUtils.hasParameterOfType(method, Sort.class)) {
				throw new IllegalStateException(String.format("Method must not have Pageable *and* Sort parameter. "
						+ "Use sorting capabilities on Pageable instead! Offending method: %s", method.toString()));
			}
		}

		this.isCollectionQueryCouchbase = Lazy.of(() -> {
			boolean result = !(isPageQuery() || isSliceQuery())
					&& ReactiveWrappers.isMultiValueType(metadata.getReturnType(method).getType());
			return result;
		});
	}

	/*
	 * All reactive query methods are streaming queries.
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#isStreamQuery()
	 */
	@Override
	public boolean isStreamQuery() {
		return true;
	}

	/*
	 * does this query return a collection?
	 * This must override QueryMethod.isCollection() as isCollectionQueryCouchbase is different from isCollectionQuery
	 *
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#isCollection()
	 */
	@Override
	public boolean isCollectionQuery() {
		return (Boolean) this.isCollectionQueryCouchbase.get();
	}
}
