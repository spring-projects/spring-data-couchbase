/*
 * Copyright 2013-2020 the original author or authors.
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
import java.util.Locale;

import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.Dimensional;
import org.springframework.data.couchbase.core.query.View;
import org.springframework.data.couchbase.core.query.WithConsistency;
import org.springframework.data.couchbase.repository.Meta;
import org.springframework.data.couchbase.repository.Query;
import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.util.ReactiveWrapperConverters;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Represents a query method with couchbase extensions, allowing to discover if View-based query or N1QL-based query
 * must be used.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @author Oliver Gierke
 * @author Michael Reiche
 */
public class CouchbaseQueryMethod extends QueryMethod {

	private final Method method;

	public CouchbaseQueryMethod(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
			MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext) {
		super(method, metadata, factory);

		this.method = method;

	}

	/**
	 * If the method has a @View annotation.
	 *
	 * @return true if it has the annotation, false otherwise.
	 */
	public boolean hasViewAnnotation() {
		return getViewAnnotation() != null;
	}

	/**
	 * If the method has a @View annotation with the designDocument and viewName specified.
	 *
	 * @return true if it has the annotation and full view specified.
	 */
	public boolean hasViewSpecification() {
		return hasDesignDoc() && hasViewName();
	}

	/**
	 * If the method has a @View annotation with the designDocument specified.
	 *
	 * @return true if it has the design document specified.
	 */
	public boolean hasDesignDoc() {
		View annotation = getViewAnnotation();
		if (annotation == null) {
			return false;
		}
		return StringUtils.hasText(annotation.designDocument());
	}

	/**
	 * If the method has a @View annotation with the viewName specified.
	 *
	 * @return true if it has the view name specified.
	 */
	public boolean hasViewName() {
		View annotation = getViewAnnotation();
		if (annotation == null) {
			return false;
		}
		return StringUtils.hasText(annotation.viewName());
	}

	/**
	 * Returns the @View annotation if set, null otherwise.
	 *
	 * @return the view annotation of present.
	 */
	public View getViewAnnotation() {
		return method.getAnnotation(View.class);
	}

	/**
	 * @return true if the method has a @Dimensional annotation, false otherwise.
	 */
	public boolean hasDimensionalAnnotation() {
		return getDimensionalAnnotation() != null;
	}

	/**
	 * @return the @Dimensional annotation if set, null otherwise.
	 */
	public Dimensional getDimensionalAnnotation() {
		return AnnotationUtils.findAnnotation(method, Dimensional.class);
	}

	/**
	 * If the method has a @Query annotation.
	 *
	 * @return true if it has the annotation, false otherwise.
	 */
	public boolean hasN1qlAnnotation() {
		return getN1qlAnnotation() != null;
	}

	/**
	 * Returns the @Query annotation if set, null otherwise.
	 *
	 * @return the n1ql annotation if present.
	 */
	public Query getN1qlAnnotation() {
		return method.getAnnotation(Query.class);
	}

	/**
	 * If the method has a @Query annotation with an inline Query statement inside.
	 *
	 * @return true if this has the annotation and N1QL inline statement, false otherwise.
	 */
	public boolean hasInlineN1qlQuery() {
		return getInlineN1qlQuery() != null;
	}

	public boolean hasConsistencyAnnotation() {
		return getConsistencyAnnotation() != null;
	}

	public WithConsistency getConsistencyAnnotation() {
		return method.getAnnotation(WithConsistency.class);
	}

	/**
	 * If the method has a @ScanConsistency annotation
	 *
	 * @return true if this has the @ScanConsistency annotation
	 */
	public boolean hasScanConsistencyAnnotation() {
		return getScanConsistencyAnnotation() != null;
	}

	/**
	 * ScanConsistency annotation
	 *
	 * @return the @ScanConsistency annotation
	 */
	public ScanConsistency getScanConsistencyAnnotation() {
		return method.getAnnotation(ScanConsistency.class);
	}

	/**
	 * @return return true if {@link Meta} annotation is available.
	 */
	public boolean hasQueryMetaAttributes() {
		return getMetaAnnotation() != null;
	}

	/**
	 * @return return {@link Meta} annotation
	 */
	private Meta getMetaAnnotation() {
		return method.getAnnotation(Meta.class);
	}

	/**
	 * Returns the {@link org.springframework.data.couchbase.core.query.Meta} attributes to be applied.
	 *
	 * @return never {@literal null}.
	 */
	@Nullable
	public org.springframework.data.couchbase.core.query.Meta getQueryMetaAttributes() {

		Meta meta = getMetaAnnotation();
		if (meta == null) {
			return new org.springframework.data.couchbase.core.query.Meta();
		}

		org.springframework.data.couchbase.core.query.Meta metaAttributes = new org.springframework.data.couchbase.core.query.Meta();

		return metaAttributes;
	}

	/**
	 * Returns the query string declared in a {@link Query} annotation or {@literal null} if neither the annotation found
	 * nor the attribute was specified.
	 *
	 * @return the query statement if present.
	 */
	public String getInlineN1qlQuery() {
		String query = (String) AnnotationUtils.getValue(getN1qlAnnotation());
		return StringUtils.hasText(query) ? query : null;
	}

	/**
	 * is this a 'delete'?
	 *
	 * @return is this a 'delete'?
	 */
	public boolean isDeleteQuery() {
		return getName().toLowerCase(Locale.ROOT).startsWith("delete");
	}

	/**
	 * is this an 'exists' query?
	 *
	 * @return is this an 'exists' query?
	 */
	public boolean isExistsQuery() {
		return getName().toLowerCase(Locale.ROOT).startsWith("exists");
	}

	/**
	 * indicates if the method begins with "count"
	 *
	 * @return true if the method begins with "count", indicating that .count() should be called instead of one() or
	 *         all().
	 */
	public boolean isCountQuery() {
		return getName().toLowerCase(Locale.ROOT).startsWith("count");
	}

	@Override
	public String toString() {
		return super.toString();
	}

	public boolean hasReactiveWrapperParameter() {
		for (Parameter p : getParameters()) {
			if (ReactiveWrapperConverters.supports(p.getType())) {
				return true;
			}
		}
		return false;
	}

}
