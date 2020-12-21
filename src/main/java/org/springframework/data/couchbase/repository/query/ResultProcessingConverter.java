/*
 * Copyright 2020 the original author or authors.
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

import org.reactivestreams.Publisher;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

final class ResultProcessingConverter<CouchbaseOperationsType> implements Converter<Object, Object> {

	private final ResultProcessor processor;
	private final CouchbaseOperationsType operations;
	private final EntityInstantiators instantiators;

	public ResultProcessingConverter(ResultProcessor processor, CouchbaseOperationsType operations,
			EntityInstantiators instantiators) {

		Assert.notNull(processor, "Processor must not be null!");
		Assert.notNull(operations, "Operations must not be null!");
		Assert.notNull(instantiators, "Instantiators must not be null!");

		this.processor = processor;
		this.operations = operations;
		this.instantiators = instantiators;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
	 */
	@Override
	public Object convert(Object source) {

		ReturnedType returnedType = processor.getReturnedType();

		if (isVoid(returnedType)) {

			if (source instanceof Mono) {
				return ((Mono<?>) source).then();
			}

			if (source instanceof Publisher) {
				return Flux.from((Publisher<?>) source).then();
			}
		}

		if (ClassUtils.isPrimitiveOrWrapper(returnedType.getReturnedType())) {
			return source;
		}

		CouchbaseConverter cvtr = operations instanceof CouchbaseOperations
				? ((CouchbaseOperations) operations).getConverter()
				: ((ReactiveCouchbaseOperations) operations).getConverter();
		if (!cvtr.getMappingContext().hasPersistentEntityFor(returnedType.getReturnedType())) {
			return source;
		}

		Converter<Object, Object> converter = new DtoInstantiatingConverter(returnedType.getReturnedType(),
				cvtr.getMappingContext(), instantiators);

		return processor.processResult(source, converter);
	}

	static boolean isVoid(ReturnedType returnedType) {
		return returnedType.getReturnedType().equals(Void.class);
	}
}
