/*
 * Copyright 2012-2017 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository.query;

import java.util.Iterator;
import java.util.Optional;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.PartTree;

import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.path.LimitPath;
import com.couchbase.client.java.query.dsl.path.WherePath;

/**
 * 
 * @author Mark Ramach
 * @author Mark Paluch
 */
public class N1qlCountQueryCreator extends N1qlQueryCreator {

	public N1qlCountQueryCreator(PartTree tree, ParameterAccessor parameters, WherePath selectFrom,
			CouchbaseConverter converter, CouchbaseQueryMethod queryMethod) {
		super(tree, new CountParameterAccessor(parameters), selectFrom, converter, queryMethod);
	}

	@Override
	protected LimitPath complete(Expression criteria, Sort sort) {
		// Sorting is not allowed on aggregate count queries.
		return super.complete(criteria, Sort.unsorted());
	}

	private static class CountParameterAccessor implements ParameterAccessor {

		private ParameterAccessor delegate;

		public CountParameterAccessor(ParameterAccessor delegate) {
			this.delegate = delegate;
		}

		public Pageable getPageable() {
			return delegate.getPageable() != Pageable.NONE ? new CountPageable(delegate.getPageable()) : Pageable.NONE;
		}

		public Sort getSort() {
			return Sort.unsorted();
		}

		public Optional<Class<?>> getDynamicProjection() {
			return delegate.getDynamicProjection();
		}

		public Object getBindableValue(int index) {
			return delegate.getBindableValue(index);
		}

		public boolean hasBindableNullValue() {
			return delegate.hasBindableNullValue();
		}

		public Iterator<Object> iterator() {
			return delegate.iterator();
		}

	}

	private static class CountPageable implements Pageable {

		private Pageable delegate;

		public CountPageable(Pageable delegate) {
			this.delegate = delegate;
		}

		public int getPageNumber() {
			return delegate.getPageNumber();
		}

		public int getPageSize() {
			return delegate.getPageSize();
		}

		public long getOffset() {
			return delegate.getOffset();
		}

		public Sort getSort() {
		  // Sorting is not allowed on aggregate count queries.
			return Sort.unsorted();
		}

		public Pageable next() {
			return delegate.next();
		}

		public Pageable previousOrFirst() {
			return delegate.previousOrFirst();
		}

		public Pageable first() {
			return delegate.first();
		}

		public boolean hasPrevious() {
			return delegate.hasPrevious();
		}

	}

}
