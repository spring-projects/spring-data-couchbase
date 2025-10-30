package org.springframework.data.couchbase.domain;

import java.util.Collections;

import org.springframework.data.convert.DefaultTypeMapper;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.couchbase.core.convert.CouchbaseTypeMapper;
import org.springframework.data.couchbase.core.convert.DefaultCouchbaseTypeMapper;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.mapping.Alias;

public class TypeBasedCouchbaseTypeMapper extends DefaultTypeMapper<CouchbaseDocument> implements CouchbaseTypeMapper {

	private final String typeKey;

	public TypeBasedCouchbaseTypeMapper(final String typeKey) {
		super(new DefaultCouchbaseTypeMapper.CouchbaseDocumentTypeAliasAccessor(typeKey),
				Collections.singletonList(new TypeAwareTypeInformationMapper()));
		this.typeKey = typeKey;
	}

	@Override
	public String getTypeKey() {
		return typeKey;
	}

	@Override
	public Alias getTypeAlias(TypeInformation<?> info) {
		return getAliasFor(info);
	}
}
