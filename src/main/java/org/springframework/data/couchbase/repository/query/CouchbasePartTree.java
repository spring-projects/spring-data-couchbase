/*
 * Copyright 2021-2025 the original author or authors.
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.data.core.PropertyPath;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * Extend PartTree to parse out distinct fields
 *
 * @author Michael Reiche
 */
public class CouchbasePartTree extends PartTree {
	private static final Pattern DISTINCT_TEMPLATE = Pattern
			.compile("^(find|read|get|query|search|stream|count)(Distinct)(\\p{Lu}.*?)(First|Top|By|$)");

	String[] distinctFields;

	public CouchbasePartTree(String methodName, Class<?> domainType) {
		super(methodName, domainType);
		maybeInitDistinctFields(methodName, domainType);
	}

	String[] getDistinctFields() {
		return distinctFields;
	}

	private void maybeInitDistinctFields(String methodName, Class<?> domainType) {
		if (isDistinct()) {
			Matcher grp = DISTINCT_TEMPLATE.matcher(methodName);
			if (grp.matches()) {
				String grp3 = grp.group(3);
				String[] names = grp.group(3).split("And");
				int parameterCount = names.length;
				distinctFields = new String[names.length];
				for (int i = 0; i < parameterCount; ++i) {
					Part.Type type = Part.Type.fromProperty(names[i]);
					PropertyPath path = PropertyPath.from(type.extractProperty(names[i]), domainType);
					distinctFields[i] = path.toDotPath();
				}
			} else {
				distinctFields = new String[0];
			}
		}
	}
}
