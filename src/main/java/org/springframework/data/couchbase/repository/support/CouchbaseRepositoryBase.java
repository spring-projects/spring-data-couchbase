/*
 * Copyright 2022 the original author or authors.
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

import java.lang.reflect.AnnotatedElement;

import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.repository.Collection;
import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.data.couchbase.repository.Scope;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;

import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.query.QueryScanConsistency;

public class CouchbaseRepositoryBase<T, ID> {

	/**
	 * Contains information about the entity being used in this repository.
	 */
	private final CouchbaseEntityInformation<T, String> entityInformation;
	private final Class<?> repositoryInterface;
	private CrudMethodMetadata crudMethodMetadata;

	public CouchbaseRepositoryBase(CouchbaseEntityInformation<T, String> entityInformation,
								   Class<?> repositoryInterface) {
		this.entityInformation = entityInformation;
		this.repositoryInterface = repositoryInterface;
	}

	/**
	 * Returns the information for the underlying template.
	 *
	 * @return the underlying entity information.
	 */
	public CouchbaseEntityInformation<T, String> getEntityInformation() {
		return entityInformation;
	}

	/**
	 * Returns the repository interface
	 *
	 * @return the underlying entity information.
	 */
	public Class<?> getRepositoryInterface() {
		return repositoryInterface;
	}

	Class<T> getJavaType() {
		return getEntityInformation().getJavaType();
	}

	<S extends T> String getId(S entity) {
		return String.valueOf(getEntityInformation().getId(entity));
	}

	/**
	 * Get the Scope from <br>
	 * 1. The repository<br>
	 * 2. The entity<br>
	 * 3. otherwise null<br>
	 * This can be overriden in the operation method by<br>
	 * 1. repository.withCollection() 2. Annotation on the method
	 */

	protected String getScope() {
		String fromAnnotation = OptionsBuilder.annotationString(Scope.class, CollectionIdentifier.DEFAULT_SCOPE,
				new AnnotatedElement[] { getJavaType(), repositoryInterface });
		String fromMetadata = crudMethodMetadata.getScope();
		return OptionsBuilder.fromFirst(CollectionIdentifier.DEFAULT_SCOPE, fromMetadata, fromAnnotation);
	}

	/**
	 * Get the Collection from <br>
	 * 1. The repository<br>
	 * 2. The entity<br>
	 * 3. otherwise null<br>
	 * This can be overriden in the operation method by<br>
	 * 1. repository.withCollection()
	 */
	protected String getCollection() {
		String fromAnnotation = OptionsBuilder.annotationString(Collection.class, CollectionIdentifier.DEFAULT_COLLECTION,
				new AnnotatedElement[] { getJavaType(), repositoryInterface });
		String fromMetadata = crudMethodMetadata.getCollection();
		return OptionsBuilder.fromFirst(CollectionIdentifier.DEFAULT_COLLECTION, fromMetadata, fromAnnotation);
	}

	/**
	 * Get the QueryScanConsistency from <br>
	 * 1. The method annotation (method *could* be available from crudMethodMetadata)<br>
	 * 2. The repository<br>
	 * 3. The entity<br>
	 * 4. otherwise null<br>
	 * This can be overriden in the operation method by<br>
	 * 1. Options.scanConsistency (?)<br>
	 * AbstractCouchbaseQueryBase.applyAnnotatedConsistencyIfPresent() <br>
	 * CouchbaseRepository get picked up? If I have the following, will the annotation be picked up?<br>
	 * Only via crudMethodMetadata<br>
	 * \@ScanConsistency(query=QueryScanConsistency.REQUEST_PLUS)<br>
	 * List<T> findAll();<br>
	 */
	QueryScanConsistency buildQueryScanConsistency() {
		ScanConsistency sc = crudMethodMetadata.getScanConsistency();
		QueryScanConsistency fromMeta = sc != null ? sc.query() : null;
		QueryScanConsistency fromAnnotation = OptionsBuilder.annotationAttribute(ScanConsistency.class, "query",
				QueryScanConsistency.NOT_BOUNDED, new AnnotatedElement[] { getJavaType(), repositoryInterface });
		return OptionsBuilder.fromFirst(QueryScanConsistency.NOT_BOUNDED, fromMeta, fromAnnotation);
	}

	/**
	 * Setter for the repository metadata, contains annotations on the overidden methods.
	 *
	 * @param crudMethodMetadata the injected repository metadata.
	 */
	void setRepositoryMethodMetadata(CrudMethodMetadata crudMethodMetadata) {
		this.crudMethodMetadata = crudMethodMetadata;
	}
}
