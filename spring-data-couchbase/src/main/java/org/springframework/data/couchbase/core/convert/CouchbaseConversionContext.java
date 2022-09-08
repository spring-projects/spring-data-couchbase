package org.springframework.data.couchbase.core.convert;

import org.springframework.beans.PropertyAccessor;
import org.springframework.data.convert.ValueConversionContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * {@link ValueConversionContext} that allows to delegate read/write to an underlying {@link CouchbaseConverter}.
 *
 * @author Christoph Strobl
 * @since 3.4
 */
public class CouchbaseConversionContext implements ValueConversionContext<CouchbasePersistentProperty> {

	private final CouchbasePersistentProperty persistentProperty;
	private final MappingCouchbaseConverter couchbaseConverter;
	private final ConvertingPropertyAccessor propertyAccessor;

	public CouchbaseConversionContext(CouchbasePersistentProperty persistentProperty,
																		MappingCouchbaseConverter couchbaseConverter, ConvertingPropertyAccessor accessor) {

		this.persistentProperty = persistentProperty;
		this.couchbaseConverter = couchbaseConverter;
		this.propertyAccessor = accessor;
	}

	@Override
	public CouchbasePersistentProperty getProperty() {
		return persistentProperty;
	}

	@Override
	public <T> T write(@Nullable Object value, TypeInformation<T> target) {
		return (T) ValueConversionContext.super.write(value, target);
	}

	@Override
	public <T> T read(@Nullable Object value, TypeInformation<T> target) {
		return ValueConversionContext.super.read(value, target);
	}

	public MappingCouchbaseConverter getConverter(){
		return couchbaseConverter;
	}

	public ConvertingPropertyAccessor getAccessor(){
		return propertyAccessor;
	}
}
