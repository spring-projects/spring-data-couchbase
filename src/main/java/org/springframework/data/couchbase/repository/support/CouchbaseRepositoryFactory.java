/*
 * Copyright 2013 the original author or authors.
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

package org.springframework.data.couchbase.repository.support;

import java.io.Serializable;
import java.lang.reflect.Method;

import com.couchbase.client.java.util.features.CouchbaseFeature;

import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.UnsupportedCouchbaseFeatureException;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;
import org.springframework.data.couchbase.core.query.N1qlSecondaryIndexed;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.View;
import org.springframework.data.couchbase.core.query.ViewIndexed;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.query.CouchbaseQueryMethod;
import org.springframework.data.couchbase.repository.query.PartTreeN1qlBasedQuery;
import org.springframework.data.couchbase.repository.query.SpatialViewBasedQuery;
import org.springframework.data.couchbase.repository.query.StringN1qlBasedQuery;
import org.springframework.data.couchbase.repository.query.ViewBasedCouchbaseQuery;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;

/**
 * Factory to create {@link SimpleCouchbaseRepository} instances.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 */
public class CouchbaseRepositoryFactory extends RepositoryFactorySupport {

  private static final SpelExpressionParser SPEL_PARSER = new SpelExpressionParser();

  /**
   * Holds the reference to the template.
   */
  private final RepositoryOperationsMapping couchbaseOperationsMapping;

  /**
   * Holds the reference to the {@link IndexManager}.
   */
  private final IndexManager indexManager;

  /**
   * Holds the mapping context.
   */
  private final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;

  /**
   * Holds a custom ViewPostProcessor..
   */
  private final ViewPostProcessor viewPostProcessor;

  /**
   * Create a new factory.
   *
   * @param couchbaseOperationsMapping the template for the underlying actions.
   */
  public CouchbaseRepositoryFactory(final RepositoryOperationsMapping couchbaseOperationsMapping, final IndexManager indexManager) {
    Assert.notNull(couchbaseOperationsMapping);
    Assert.notNull(indexManager);

    this.couchbaseOperationsMapping = couchbaseOperationsMapping;
    this.indexManager = indexManager;
    mappingContext = this.couchbaseOperationsMapping.getDefault().getConverter().getMappingContext();
    viewPostProcessor = ViewPostProcessor.INSTANCE;

    addRepositoryProxyPostProcessor(viewPostProcessor);
  }

  /**
   * Returns entity information based on the domain class.
   *
   * @param domainClass the class for the entity.
   * @param <T> the value type
   * @param <ID> the id type.
   *
   * @return entity information for that domain class.
   */
  @Override
  public <T, ID extends Serializable> CouchbaseEntityInformation<T, ID> getEntityInformation(final Class<T> domainClass) {
    CouchbasePersistentEntity<?> entity = mappingContext.getPersistentEntity(domainClass);

    if (entity == null) {
      throw new MappingException(String.format("Could not lookup mapping metadata for domain class %s!", domainClass.getName()));
    }

    return new MappingCouchbaseEntityInformation<T, ID>((CouchbasePersistentEntity<T>) entity);
  }

  /**
   * Returns a new Repository based on the metadata.
   *
   * @param metadata the repository metadata.
   *
   * @return a new created repository.
   */
  @Override
  protected Object getTargetRepository(final RepositoryInformation metadata) {
    CouchbaseOperations couchbaseOperations = couchbaseOperationsMapping.resolve(metadata.getRepositoryInterface(), metadata.getDomainType());
    boolean isN1qlAvailable = couchbaseOperations.getCouchbaseClusterInfo().checkAvailable(CouchbaseFeature.N1QL);

    ViewIndexed viewIndexed = AnnotationUtils.findAnnotation(metadata.getRepositoryInterface(), ViewIndexed.class);
    N1qlPrimaryIndexed n1qlPrimaryIndexed = AnnotationUtils.findAnnotation(metadata.getRepositoryInterface(), N1qlPrimaryIndexed.class);
    N1qlSecondaryIndexed n1qlSecondaryIndexed = AnnotationUtils.findAnnotation(metadata.getRepositoryInterface(), N1qlSecondaryIndexed.class);

    checkFeatures(metadata, isN1qlAvailable, n1qlPrimaryIndexed, n1qlSecondaryIndexed);

    indexManager.buildIndexes(metadata, viewIndexed, n1qlPrimaryIndexed, n1qlSecondaryIndexed, couchbaseOperations);

    CouchbaseEntityInformation<?, Serializable> entityInformation = getEntityInformation(metadata.getDomainType());
    if (isN1qlAvailable) {
      //this implementation also conforms to PagingAndSortingRepository
      N1qlCouchbaseRepository n1qlRepository = new N1qlCouchbaseRepository(entityInformation, couchbaseOperations);
      n1qlRepository.setViewMetadataProvider(viewPostProcessor.getViewMetadataProvider());
      return n1qlRepository;
    } else {
      final SimpleCouchbaseRepository simpleCouchbaseRepository = new SimpleCouchbaseRepository(entityInformation, couchbaseOperations);
      simpleCouchbaseRepository.setViewMetadataProvider(viewPostProcessor.getViewMetadataProvider());
      return simpleCouchbaseRepository;
    }
  }

  private void checkFeatures(RepositoryInformation metadata, boolean isN1qlAvailable,
                             N1qlPrimaryIndexed n1qlPrimaryIndexed, N1qlSecondaryIndexed n1qlSecondaryIndexed) {
    //paging repo will always need N1QL, also check if the repository requires a N1QL index
    boolean needsN1ql = metadata.isPagingRepository() || n1qlPrimaryIndexed != null || n1qlSecondaryIndexed != null;

    //for other repos, they might also need N1QL if they don't have only @View methods
    if (!needsN1ql) {

      for (Method method : metadata.getQueryMethods()) {

        boolean hasN1ql = AnnotationUtils.findAnnotation(method, Query.class) != null;
        boolean hasView = AnnotationUtils.findAnnotation(method, View.class) != null;

        if (hasN1ql || !hasView) {
          needsN1ql = true;
          break;
        }
      }
    }

    if (needsN1ql && !isN1qlAvailable) {
      throw new UnsupportedCouchbaseFeatureException("Repository uses N1QL", CouchbaseFeature.N1QL);
    }
  }

  /**
   * The base class for this repository.
   *
   * @param repositoryMetadata metadata for the repository.
   *
   * @return the base class.
   */
  @Override
  protected Class<?> getRepositoryBaseClass(final RepositoryMetadata repositoryMetadata) {
    CouchbaseOperations couchbaseOperations = couchbaseOperationsMapping.resolve(repositoryMetadata.getRepositoryInterface(),
        repositoryMetadata.getDomainType());
    boolean isN1qlAvailable = couchbaseOperations.getCouchbaseClusterInfo().checkAvailable(CouchbaseFeature.N1QL);
    if (isN1qlAvailable) {
      return N1qlCouchbaseRepository.class;
    }
    return SimpleCouchbaseRepository.class;
  }

  @Override
  protected QueryLookupStrategy getQueryLookupStrategy(QueryLookupStrategy.Key key, EvaluationContextProvider contextProvider) {
    return new CouchbaseQueryLookupStrategy(contextProvider);
  }

  /**
   * Strategy to lookup Couchbase queries implementation to be used.
   */
  private class CouchbaseQueryLookupStrategy implements QueryLookupStrategy {

    private final EvaluationContextProvider evaluationContextProvider;

    public CouchbaseQueryLookupStrategy(EvaluationContextProvider evaluationContextProvider) {
      this.evaluationContextProvider = evaluationContextProvider;
    }

    @Override
    public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, NamedQueries namedQueries) {
      CouchbaseOperations couchbaseOperations = couchbaseOperationsMapping.resolve(metadata.getRepositoryInterface(),
          metadata.getDomainType());

      CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, mappingContext);
      String namedQueryName = queryMethod.getNamedQueryName();

      if (queryMethod.hasDimensionalAnnotation()) {
        return new SpatialViewBasedQuery(queryMethod, couchbaseOperations);
      } else if (queryMethod.hasViewAnnotation()) {
        return new ViewBasedCouchbaseQuery(queryMethod, couchbaseOperations);
      } else if (queryMethod.hasN1qlAnnotation()) {
        if (queryMethod.hasInlineN1qlQuery()) {
          return new StringN1qlBasedQuery(queryMethod.getInlineN1qlQuery(), queryMethod, couchbaseOperations,
              SPEL_PARSER, evaluationContextProvider);
        } else if (namedQueries.hasQuery(namedQueryName)) {
          String namedQuery = namedQueries.getQuery(namedQueryName);
          return new StringN1qlBasedQuery(namedQuery, queryMethod, couchbaseOperations,
              SPEL_PARSER, evaluationContextProvider);
        } //otherwise will do default, queryDerivation
      }
      return new PartTreeN1qlBasedQuery(queryMethod, couchbaseOperations);
    }
  }

}
