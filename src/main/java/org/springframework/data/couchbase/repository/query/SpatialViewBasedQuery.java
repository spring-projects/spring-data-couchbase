/*
 * Copyright 2013-2015 the original author or authors.
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

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.query.Dimensional;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * Execute a {@link Dimensional} repository query through the Spatial View mechanism.
 *
 * @author Simon Baslé
 */
public class SpatialViewBasedQuery implements RepositoryQuery {

  private static final Logger LOG = LoggerFactory.getLogger(SpatialViewBasedQuery.class);

  private final CouchbaseQueryMethod method;
  private final CouchbaseOperations operations;

  public SpatialViewBasedQuery(CouchbaseQueryMethod method, CouchbaseOperations operations) {
    this.method = method;
    this.operations = operations;
  }

  @Override
  public Object execute(Object[] runtimeParams) {
    String designDoc = method.getDimensionalAnnotation().designDocument();
    String viewName = method.getDimensionalAnnotation().spatialViewName();
    int dimensions = method.getDimensionalAnnotation().dimensions();

    /*
      here contrary to the classical view query we don't support not including an attribute of
      the entity in the method name, those are mandatory and will result in a PropertyReferenceException
      if not used...
     */
    PartTree tree = new PartTree(method.getName(), method.getEntityInformation().getJavaType());

    //prepare a spatial view query to be used as a base for the query creator
    SpatialViewQuery baseSpatialQuery = SpatialViewQuery.from(designDoc, viewName)
        .stale(operations.getDefaultConsistency().viewConsistency());

    //use the SpatialViewQueryCreator to complete it
    SpatialViewQueryCreator creator = new SpatialViewQueryCreator(dimensions,
        tree, new ParametersParameterAccessor(method.getParameters(), runtimeParams),
        baseSpatialQuery, operations.getConverter());
    SpatialViewQuery finalQuery = creator.createQuery();

    //execute the spatial query
    return execute(finalQuery);
  }

  protected Object execute(SpatialViewQuery query) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Executing spatial view query: " + query.toString());
    }
    return operations.findBySpatialView(query, method.getEntityInformation().getJavaType());
  }

  @Override
  public CouchbaseQueryMethod getQueryMethod() {
    return method;
  }
}