package org.springframework.data.couchbase.repository.support;

import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.repository.ScanConsistency;
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

	Class<T> getJavaType() {
		return getEntityInformation().getJavaType();
	}

	<S extends T> String getId(S entity) {
		return getEntityInformation().getId(entity);
	}

	/**
	 * Get the Scope from <br>
	 * 1. The repository<br>
	 * 2. The entity<br>
	 * 3. otherwise null<br>
	 * This can be overriden in the operation method by<br>
	 * 1. repository.withCollection() 2. Annotation on the method
	 */

	String getScope() {
		String entityScope = getJavaType().getAnnotation(Document.class) != null
				? getJavaType().getAnnotation(Document.class).scope()
				: null;
		String interfaceScope = repositoryInterface.getAnnotation(Document.class) != null
				? repositoryInterface.getAnnotation(Document.class).scope()
				: null;
		return entityScope != null && !CollectionIdentifier.DEFAULT_SCOPE.equals(entityScope) ? entityScope
				: (interfaceScope != null && !interfaceScope.equals(CollectionIdentifier.DEFAULT_SCOPE) ? interfaceScope
						: null);
	}

	/**
	 * Get the Collection from <br>
	 * 1. The repository<br>
	 * 2. The entity<br>
	 * 3. otherwise null<br>
	 * This can be overriden in the operation method by<br>
	 * 1. repository.withCollection()
	 */
	String getCollection() {
		String entityCollection = getJavaType().getAnnotation(Document.class) != null
				? getJavaType().getAnnotation(Document.class).collection()
				: null;
		String interfaceCollection = repositoryInterface.getAnnotation(Document.class) != null
				? repositoryInterface.getAnnotation(Document.class).collection()
				: null;
		return entityCollection != null && !CollectionIdentifier.DEFAULT_COLLECTION.equals(entityCollection)
				? entityCollection
				: (interfaceCollection != null && !interfaceCollection.equals(CollectionIdentifier.DEFAULT_COLLECTION)
						? interfaceCollection
						: null);
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
	 * TODO: Where can the annotation on an override in the repository interface of a method in<br>
	 * CouchbaseRepository get picked up? If I have the following, will the annotation be picked up?<br>
	 * Only via crudMethodMetadata<br>
	 * \@ScanConsistency(query=QueryScanConsistency.REQUEST_PLUS)<br>
	 * List<T> findAll();<br>
	 */
	QueryScanConsistency buildQueryScanConsistency() {
		try {
			QueryScanConsistency scanConsistency;
			ScanConsistency sc = crudMethodMetadata.getScanConsistency();
			scanConsistency = sc != null ? sc.query() : null;
			if (scanConsistency != null && scanConsistency != QueryScanConsistency.NOT_BOUNDED) {
				return scanConsistency;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		QueryScanConsistency entityConsistency = getJavaType().getAnnotation(ScanConsistency.class) != null
				? getJavaType().getAnnotation(ScanConsistency.class).query()
				: null;
		QueryScanConsistency interfaceCollection = repositoryInterface.getAnnotation(ScanConsistency.class) != null
				? repositoryInterface.getAnnotation(ScanConsistency.class).query()
				: null;
		return entityConsistency != null && entityConsistency != QueryScanConsistency.NOT_BOUNDED ? entityConsistency
				: (interfaceCollection != null && interfaceCollection != QueryScanConsistency.NOT_BOUNDED ? interfaceCollection
						: null);
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
