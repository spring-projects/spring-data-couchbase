/*
 * Copyright 2013-2022 the original author or authors.
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

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.Locale;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.Dimensional;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.core.query.View;
import org.springframework.data.couchbase.core.query.WithConsistency;
import org.springframework.data.couchbase.repository.Collection;
import org.springframework.data.couchbase.repository.Query;
import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.data.couchbase.repository.Scope;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.util.ReactiveWrapperConverters;
import org.springframework.util.StringUtils;

import com.couchbase.client.core.io.CollectionIdentifier;

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
	private final RepositoryMetadata repositoryMetadata;

	public CouchbaseQueryMethod(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
			MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext) {
		super(method, metadata, factory);
		this.method = method;
		this.repositoryMetadata = metadata;
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
		AnnotatedElement[] annotated = new AnnotatedElement[] { method, method.getDeclaringClass(),
				repositoryMetadata.getRepositoryInterface(), repositoryMetadata.getDomainType() };
		return OptionsBuilder.annotation(ScanConsistency.class, "query", CollectionIdentifier.DEFAULT_COLLECTION,
				annotated);
	}

	/**
	 * Caution: findMergedAnnotation() will return the default if there are any annotations but not this annotation
	 * 
	 * @return annotation
	 */
	public <A extends Annotation> A getAnnotation(Class<A> annotationClass) {
		return AnnotatedElementUtils.findMergedAnnotation(method, annotationClass);
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

	public String getCollection() {
		// Try the repository method, then the repository class, then the entity class
		AnnotatedElement[] annotated = new AnnotatedElement[] { method, method.getDeclaringClass(),
				repositoryMetadata.getRepositoryInterface(), repositoryMetadata.getDomainType() };
		return OptionsBuilder.annotationString(Collection.class, CollectionIdentifier.DEFAULT_COLLECTION, annotated);
	}

	public String getScope() {
		// Try the repository method, then the repository class, then the entity class
		AnnotatedElement[] annotated = new AnnotatedElement[] { method, method.getDeclaringClass(),
				repositoryMetadata.getRepositoryInterface(), repositoryMetadata.getDomainType() };
		return OptionsBuilder.annotationString(Scope.class, CollectionIdentifier.DEFAULT_SCOPE, annotated);
	}

}
