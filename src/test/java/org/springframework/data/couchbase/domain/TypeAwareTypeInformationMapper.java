package org.springframework.data.couchbase.domain;

import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.convert.SimpleTypeInformationMapper;
import org.springframework.data.mapping.Alias;
import org.springframework.data.util.TypeInformation;

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
