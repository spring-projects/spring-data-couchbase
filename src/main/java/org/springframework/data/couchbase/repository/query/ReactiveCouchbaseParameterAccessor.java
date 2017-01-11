/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository.query;


import java.util.ArrayList;
import java.util.List;

import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.util.ReactiveWrapperConverters;
import org.springframework.data.repository.util.ReactiveWrappers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Reactive {@link org.springframework.data.repository.query.ParametersParameterAccessor} implementation that subscribes
 * to reactive parameter wrapper types upon creation. This class performs synchronization when accessing parameters.
 *
 * @author Subhashni Balakrishnan
 * @since 3.0
 */
public class ReactiveCouchbaseParameterAccessor extends ParametersParameterAccessor {

	private final Object[] values;
	private final List<MonoProcessor<?>> subscriptions;

	public ReactiveCouchbaseParameterAccessor(CouchbaseQueryMethod method, Object[] values) {
		super(method.getParameters(), values);
		this.values = values;
		this.subscriptions = new ArrayList<>(values.length);

		for (int i = 0; i < values.length; i++) {

			Object value = values[i];

			if (value == null || !ReactiveWrappers.supports(value.getClass())) {
				subscriptions.add(null);
				continue;
			}

			if (ReactiveWrappers.isSingleValueType(value.getClass())) {
				subscriptions.add(ReactiveWrapperConverters.toWrapper(value, Mono.class).subscribe());
			} else {
				subscriptions.add(ReactiveWrapperConverters.toWrapper(value, Flux.class).collectList().subscribe());
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParametersParameterAccessor#getValue(int)
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected <T> T getValue(int index) {

		if (subscriptions.get(index) != null) {
			return (T) subscriptions.get(index).block();
		}

		return super.getValue(index);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParametersParameterAccessor#getBindableValue(int)
	 */
	public Object getBindableValue(int index) {
		return getValue(getParameters().getBindableParameter(index).getIndex());
	}
}
