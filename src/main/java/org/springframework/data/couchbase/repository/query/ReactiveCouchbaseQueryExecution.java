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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.reactivestreams.Publisher;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Set of classes to contain query execution strategies. Depending (mostly) on the return type of a
 * {@link org.springframework.data.repository.query.QueryMethod} a {@link AbstractReactiveCouchbaseQuery} can be
 * executed in various flavors.
 *
 * @author Michael Reiche
 * @since 4.1
 */
interface ReactiveCouchbaseQueryExecution {

	Object execute(Query query, Class<?> type, String collection);

	/**
	 * {@link ReactiveCouchbaseQueryExecution} removing documents matching the query.
	 *
	 * @author Mark Paluch
	 * @author Artyom Gabeev
	 */

	final class DeleteExecution implements ReactiveCouchbaseQueryExecution {

		private final ReactiveCouchbaseOperations operations;
		private final CouchbaseQueryMethod method;

		public DeleteExecution(ReactiveCouchbaseOperations operations, CouchbaseQueryMethod method) {
			this.operations = operations;
			this.method = method;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.couchbase.repository.query.AbstractCouchbaseQuery.Execution#execute(org.springframework.data.couchbase.core.query.Query, java.lang.Class, java.lang.String)
		 */

		@Override
		public Object execute(Query query, Class<?> type, String collection) {
			return operations.removeByQuery(type).inCollection(collection).matching(query).all();
		}

	}

	/**
	 * An {@link ReactiveCouchbaseQueryExecution} that wraps the results of the given delegate with the given result
	 * processing.
	 */
	final class ResultProcessingExecution implements ReactiveCouchbaseQueryExecution {

		private final ReactiveCouchbaseQueryExecution delegate;
		private final Converter<Object, Object> converter;

		public ResultProcessingExecution(ReactiveCouchbaseQueryExecution delegate, Converter<Object, Object> converter) {

			Assert.notNull(delegate, "Delegate must not be null!");
			Assert.notNull(converter, "Converter must not be null!");

			this.delegate = delegate;
			this.converter = converter;
		}

		@Override
		public Object execute(Query query, Class<?> type, String collection) {
			return converter.convert(delegate.execute(query, type, collection));
		}
	}

	/**
	 * A {@link Converter} to post-process all source objects using the given {@link ResultProcessor}.
	 *
	 * @author Mark Paluch
	 */
	final class ResultProcessingConverter implements Converter<Object, Object> {

		private final ResultProcessor processor;
		private final ReactiveCouchbaseOperations operations;
		private final EntityInstantiators instantiators;

		public ResultProcessingConverter(ResultProcessor processor, ReactiveCouchbaseOperations operations,
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

			if (!operations.getConverter().getMappingContext().hasPersistentEntityFor(returnedType.getReturnedType())) {
				return source;
			}

			Converter<Object, Object> converter = new DtoInstantiatingConverter(returnedType.getReturnedType(),
					operations.getConverter().getMappingContext(), instantiators);

			return processor.processResult(source, converter);
		}
	}

	static boolean isVoid(ReturnedType returnedType) {
		return returnedType.getReturnedType().equals(Void.class);
	}
}
