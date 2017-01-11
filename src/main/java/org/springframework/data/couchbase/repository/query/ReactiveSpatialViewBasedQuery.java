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

import com.couchbase.client.java.view.SpatialViewQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.RxJavaCouchbaseOperations;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.repository.util.ReactiveWrapperConverters;
import reactor.core.publisher.Flux;

/**
 * A reactive {@link RepositoryQuery} for Couchbase, for spatial queries
 *
 * @author Subhashni Balakrishnan
 * @since 3.0
 */
public class ReactiveSpatialViewBasedQuery implements RepositoryQuery {
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveSpatialViewBasedQuery.class);

	private final CouchbaseQueryMethod method;
	private final RxJavaCouchbaseOperations operations;

	public ReactiveSpatialViewBasedQuery(CouchbaseQueryMethod method, RxJavaCouchbaseOperations operations) {
		this.method = method;
		this.operations = operations;
	}

	@Override
	public Object execute(Object[] runtimeParams) {
		String designDoc = method.getDimensionalAnnotation().designDocument();
		String viewName = method.getDimensionalAnnotation().spatialViewName();
		int dimensions = method.getDimensionalAnnotation().dimensions();
		PartTree tree = new PartTree(method.getName(), method.getEntityInformation().getJavaType());

		//prepare a spatial view query to be used as a base for the query creator
		SpatialViewQuery baseSpatialQuery = SpatialViewQuery.from(designDoc, viewName)
				.stale(operations.getDefaultConsistency().viewConsistency());

		//use the SpatialViewQueryCreator to complete it
		SpatialViewQueryCreator creator = new SpatialViewQueryCreator(dimensions,
				tree, new ReactiveCouchbaseParameterAccessor(method, runtimeParams),
				baseSpatialQuery, operations.getConverter());
		SpatialViewQueryCreator.SpatialViewQueryWrapper finalQuery = creator.createQuery();

		//execute the spatial query
		return execute(finalQuery);
	}

	protected Object execute(SpatialViewQueryCreator.SpatialViewQueryWrapper query) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Executing spatial view query: " + query.getQuery().toString());
		}

		//TODO: eliminate false positives in geo query
		return ReactiveWrapperConverters.toWrapper(operations.findBySpatialView(query.getQuery(),
				method.getEntityInformation().getJavaType()), Flux.class);
	}

	@Override
	public CouchbaseQueryMethod getQueryMethod() {
		return method;
	}
}
