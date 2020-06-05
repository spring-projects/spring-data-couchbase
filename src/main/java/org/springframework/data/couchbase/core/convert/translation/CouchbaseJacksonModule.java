/*
 * Copyright 2020 the original author or authors
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
package org.springframework.data.couchbase.core.convert.translation;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.ContextualSerializer;
import com.fasterxml.jackson.databind.ser.ResolvableSerializer;
import com.fasterxml.jackson.databind.ser.std.StdDelegatingSerializer;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.databind.util.Converter;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbaseList;

import java.util.List;
import java.util.Map;

/**
 * CouchbaseJacksonModule was provided by spac-valentin to handle nested List (CouchbaseList)
 * and Map (CouchbaseDocument).
 * The CouchbaseListSerializer and CouchbaseDocumentSerializer classes have been modified to extend the
 * {@link StdDelegatingSerializer}
 *
 * @author spac-valentin
 * @author Michael Reiche
 */
public class CouchbaseJacksonModule extends SimpleModule {

	public CouchbaseJacksonModule() {
		super(new Version(1, 0, 0, null, "com.couchbase", "CouchbaseDocumentAndList"));
		addSerializer(CouchbaseDocument.class, new CouchbaseDocumentSerializer(new CouchbaseDocumentConverter()));
		addSerializer(CouchbaseList.class, new CouchbaseListSerializer(new CouchbaseListConverter()));
	}

	public static class CouchbaseDocumentSerializer extends StdDelegatingSerializer implements ResolvableSerializer,
			ContextualSerializer {

		public CouchbaseDocumentSerializer(Converter<?, ?> converter) {
			super(converter);
		}

		@Override
		protected StdDelegatingSerializer withDelegate(Converter<Object, ?> converter, JavaType delegateType, JsonSerializer<?> delegateSerializer) {
			return new StdDelegatingSerializer(converter, delegateType, delegateSerializer);
		}

	}

	public static class CouchbaseListSerializer extends StdDelegatingSerializer implements ResolvableSerializer,
			ContextualSerializer {

		public CouchbaseListSerializer(Converter<?, ?> converter) {
			super(converter);
		}

		@Override
		protected StdDelegatingSerializer withDelegate(Converter<Object, ?> converter, JavaType delegateType, JsonSerializer<?> delegateSerializer) {
			return new StdDelegatingSerializer(converter, delegateType, delegateSerializer);
		}
	}

	public static class CouchbaseListConverter implements Converter<Object,Object>{

		@Override public Object convert(Object o) {
			return ((CouchbaseList)o).export();
		}

		@Override public JavaType getInputType(TypeFactory typeFactory) {
			return typeFactory.constructType(CouchbaseList.class);
		}

		@Override public JavaType getOutputType(TypeFactory typeFactory) {
			return typeFactory.constructType(List.class);
		}
	}

	public static class CouchbaseDocumentConverter implements Converter<Object,Object>{

		@Override public Object convert(Object o) {
			return ((CouchbaseDocument)o).export();
		}

		@Override public JavaType getInputType(TypeFactory typeFactory) {
			return typeFactory.constructType(CouchbaseDocument.class);
		}

		@Override public JavaType getOutputType(TypeFactory typeFactory) {
			return typeFactory.constructType(Map.class);
		}
	}
}
