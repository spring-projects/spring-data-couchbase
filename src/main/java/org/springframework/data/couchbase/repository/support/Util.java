package org.springframework.data.couchbase.repository.support;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;

public class Util {

	public static boolean hasNonZeroVersionProperty(Object entity, CouchbaseConverter converter) {
		CouchbasePersistentEntity<?> mapperEntity = converter.getMappingContext().getPersistentEntity(entity.getClass());
		final CouchbasePersistentProperty versionProperty = mapperEntity.getVersionProperty();
		boolean hasVersionProperty = false;
		try {
			if (versionProperty != null && versionProperty.getField() != null) {
				Object versionValue = versionProperty.getField().get(entity);
				hasVersionProperty = versionValue != null && !versionValue.equals(Long.valueOf(0));
			}
		} catch (IllegalAccessException iae) {}
		return hasVersionProperty;
	}
}
