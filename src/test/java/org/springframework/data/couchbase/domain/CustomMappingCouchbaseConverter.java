package org.springframework.data.couchbase.domain;

import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.mapping.context.MappingContext;

public class CustomMappingCouchbaseConverter extends MappingCouchbaseConverter {

	/**
	 * this constructer creates a TypeBasedCouchbaseTypeMapper with the specified typeKey
	 * while MappingCouchbaseConverter uses a DefaultCouchbaseTypeMapper
	 *    typeMapper = new DefaultCouchbaseTypeMapper(typeKey != null ? typeKey : TYPEKEY_DEFAULT);
	 * @param mappingContext
	 * @param typeKey - the typeKey to be used (normally "_class")
	 */
	public CustomMappingCouchbaseConverter(final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext, final String typeKey) {
		super(mappingContext, typeKey);
		this.typeMapper = new TypeBasedCouchbaseTypeMapper(typeKey);
	}

}
