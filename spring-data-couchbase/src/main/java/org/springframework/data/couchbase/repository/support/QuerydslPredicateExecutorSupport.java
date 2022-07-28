/*
 * Copyright 2012-2022 the original author or authors
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
package org.springframework.data.couchbase.repository.support;

import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.dsl.PathBuilder;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.domain.Sort;
import org.springframework.data.querydsl.QSort;
import org.springframework.data.repository.core.EntityInformation;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Michael Reiche
 */
public class QuerydslPredicateExecutorSupport<T> {

	private final SpringDataCouchbaseSerializer serializer;
	private final PathBuilder<T> builder;
	private final EntityInformation<T, ?> entityInformation;

	QuerydslPredicateExecutorSupport(CouchbaseConverter converter, PathBuilder<T> builder,
			EntityInformation<T, ?> entityInformation) {

		this.serializer = new SpringDataCouchbaseSerializer(converter);
		this.builder = builder;
		this.entityInformation = entityInformation;
	}

	protected static <E> PathBuilder<E> pathBuilderFor(EntityPath<E> path) {
		return new PathBuilder<>(path.getType(), path.getMetadata());
	}

	protected EntityInformation<T, ?> typeInformation() {
		return entityInformation;
	}

	protected SpringDataCouchbaseSerializer cuchbaseSerializer() {
		return serializer;
	}

	/**
	 * Transforms a plain {@link Sort.Order} into a Querydsl specific {@link OrderSpecifier}.
	 *
	 * @param order
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected OrderSpecifier<?> toOrder(Sort.Order order) {

		Expression<Object> property = builder.get(order.getProperty());

		return new OrderSpecifier(
				order.isAscending() ? com.querydsl.core.types.Order.ASC : com.querydsl.core.types.Order.DESC, property);
	}

	/**
	 * Converts the given {@link Sort} to {@link OrderSpecifier}.
	 *
	 * @param sort
	 * @return
	 */
	protected List<OrderSpecifier<?>> toOrderSpecifiers(Sort sort) {

		if (sort instanceof QSort) {
			return ((QSort) sort).getOrderSpecifiers();
		}

		return sort.stream().map(this::toOrder).collect(Collectors.toList());
	}
}
