/*
 * Copyright 2012-present the original author or authors
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
package org.springframework.data.couchbase.core.convert;

import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.convert.SimpleTypeInformationMapper;
import org.springframework.data.mapping.Alias;
import org.springframework.data.util.TypeInformation;

/**
 * TypeAwareTypeInformationMapper - leverages @TypeAlias
 *
 * @author Michael Reiche
 */
public class TypeAwareTypeInformationMapper extends SimpleTypeInformationMapper {

	@Override
	public Alias createAliasFor(TypeInformation<?> type) {
		TypeAlias[] typeAlias = type.getType().getAnnotationsByType(TypeAlias.class);

		if (typeAlias.length == 1) {
			return Alias.of(typeAlias[0].value());
		}

		return super.createAliasFor(type);
	}
}
