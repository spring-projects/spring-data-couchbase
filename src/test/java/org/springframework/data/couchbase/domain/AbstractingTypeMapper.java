/*
 * Copyright 2022-present the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.domain;

import java.util.Collections;

import org.springframework.data.convert.DefaultTypeMapper;
import org.springframework.data.convert.TypeAliasAccessor;
import org.springframework.data.couchbase.core.convert.CouchbaseTypeMapper;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.mapping.Alias;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.util.TypeInformation;

/**
 * TypeMapper that leverages subtype 
 *
 * @author Michael Reiche
 */
public class AbstractingTypeMapper extends DefaultTypeMapper<CouchbaseDocument> implements CouchbaseTypeMapper {

	public static final String SUBTYPE = "subtype";
	private final String typeKey;

	public static class Type {
		public static final String ABSTRACTUSER = "abstractuser", USER = "user", OTHERUSER = "otheruser";
	}

	/**
	 * Create a new type mapper with the type key.
	 *
	 * @param typeKey the typeKey to use.
	 */
	public AbstractingTypeMapper(final String typeKey) {
		super(new CouchbaseDocumentTypeAliasAccessor(typeKey), (MappingContext) null, Collections
				.singletonList(new org.springframework.data.couchbase.core.convert.TypeAwareTypeInformationMapper()));
		this.typeKey = typeKey;
	}

	@Override
	public String getTypeKey() {
		return this.typeKey;
	}

	public static final class CouchbaseDocumentTypeAliasAccessor implements TypeAliasAccessor<CouchbaseDocument> {

		private final String typeKey;

		public CouchbaseDocumentTypeAliasAccessor(final String typeKey) {
			this.typeKey = typeKey;
		}

		@Override
		public Alias readAliasFrom(final CouchbaseDocument source) {
			String alias = (String) source.get(typeKey);
			if (Type.ABSTRACTUSER.equals(alias)) {
				String subtype = (String) source.get(AbstractingTypeMapper.SUBTYPE);
				if (Type.OTHERUSER.equals(subtype)) {
					alias = OtherUser.class.getName();
				} else if (Type.USER.equals(subtype)) {
					alias = User.class.getName();
				} else {
					throw new RuntimeException(
							"no mapping for type " + SUBTYPE + "=" + subtype + " in type " + alias + " source=" + source);
				}
			}
			return Alias.ofNullable(alias);
		}

		@Override
		public void writeTypeTo(final CouchbaseDocument sink, final Object alias) {
			if (typeKey != null) {
				sink.put(typeKey, alias);
			}
		}
	}

	@Override
	public Alias getTypeAlias(TypeInformation<?> info) {
		return getAliasFor(info);
	}

}
