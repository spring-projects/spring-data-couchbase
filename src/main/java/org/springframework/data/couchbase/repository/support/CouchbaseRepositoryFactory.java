/*
 * Copyright 2013-2019 the original author or authors.
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

package org.springframework.data.couchbase.repository.support;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Optional;

import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;
import org.springframework.data.couchbase.core.query.N1qlSecondaryIndexed;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.query.CouchbaseQueryMethod;
import org.springframework.data.couchbase.repository.query.PartTreeN1qlBasedQuery;
import org.springframework.data.couchbase.repository.query.StringN1qlBasedQuery;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;

/**
 * Factory to create {@link SimpleCouchbaseRepository} instances.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @author Oliver Gierke
 * @author Mark Paluch
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
   * Create a new factory.
   *
   * @param couchbaseOperationsMapping the template for the underlying actions.
   */
  public CouchbaseRepositoryFactory(final RepositoryOperationsMapping couchbaseOperationsMapping, final IndexManager indexManager) {
    Assert.notNull(couchbaseOperationsMapping, "RepositoryOperationsMapping must not be null!");
    Assert.notNull(indexManager, "IndexManager must not be null!");

    this.couchbaseOperationsMapping = couchbaseOperationsMapping;
    this.indexManager = indexManager;
    mappingContext = this.couchbaseOperationsMapping.getMappingContext();
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
  public <T, ID> CouchbaseEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {

    CouchbasePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(domainClass);
    return new MappingCouchbaseEntityInformation<T, ID>((CouchbasePersistentEntity<T>) entity);
  }

  /**
   * Returns a new Repository based on the metadata. Two categories of repositories can be instantiated:
   * {@link SimpleCouchbaseRepository}.
   *
   * This method performs feature checks to decide which of the two categories can be instantiated (eg. is N1QL available?).
   * Instantiation is done via reflection, see {@link #getRepositoryBaseClass(RepositoryMetadata)}.
   *
   * @param metadata the repository metadata.
   *
   * @return a new created repository.
   */
  @Override
  protected final Object getTargetRepository(final RepositoryInformation metadata) {
    CouchbaseOperations couchbaseOperations = couchbaseOperationsMapping.resolve(metadata.getRepositoryInterface(), metadata.getDomainType());

    // TODO: we really require the primary index now -- lets ponder this a bit.
    N1qlPrimaryIndexed n1qlPrimaryIndexed = AnnotationUtils.findAnnotation(metadata.getRepositoryInterface(), N1qlPrimaryIndexed.class);
    N1qlSecondaryIndexed n1qlSecondaryIndexed = AnnotationUtils.findAnnotation(metadata.getRepositoryInterface(), N1qlSecondaryIndexed.class);

    indexManager.buildIndexes(metadata, n1qlPrimaryIndexed, n1qlSecondaryIndexed, couchbaseOperations);

    CouchbaseEntityInformation<?, Serializable> entityInformation = getEntityInformation(metadata.getDomainType());
    SimpleCouchbaseRepository repo = getTargetRepositoryViaReflection(metadata, entityInformation, couchbaseOperations);
    return repo;
  }

  /**
   * Returns the base class for the repository being constructed.  We always return
   * Override these methods if you want to change the base class for all your repositories.
   *
   * @param repositoryMetadata metadata for the repository.
   *
   * @return the base class.
   */
  @Override
  protected final Class<?> getRepositoryBaseClass(final RepositoryMetadata repositoryMetadata) {
    // TODO: ponder this in more detail.
    // Since we now use n1ql in SimpleCouchbaseRepository, there is no real reason not to just return
    // the N1qlRepository in all cases.  In which case, we don't really need to have both, at all.  For
    // now lets just return it, and later we can combine them into one.
    return SimpleCouchbaseRepository.class;
  }

  @Override
  protected Optional<QueryLookupStrategy> getQueryLookupStrategy(QueryLookupStrategy.Key key, QueryMethodEvaluationContextProvider contextProvider) {
    return Optional.of(new CouchbaseQueryLookupStrategy(contextProvider));
  }

  /**
   * Strategy to lookup Couchbase queries implementation to be used.
   */
  private class CouchbaseQueryLookupStrategy implements QueryLookupStrategy {

    private final QueryMethodEvaluationContextProvider evaluationContextProvider;

    public CouchbaseQueryLookupStrategy(QueryMethodEvaluationContextProvider evaluationContextProvider) {
      this.evaluationContextProvider = evaluationContextProvider;
    }

    @Override
    public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory, NamedQueries namedQueries) {
      CouchbaseOperations couchbaseOperations = couchbaseOperationsMapping.resolve(metadata.getRepositoryInterface(),
          metadata.getDomainType());

      CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, factory, mappingContext);
      String namedQueryName = queryMethod.getNamedQueryName();

      if (queryMethod.hasN1qlAnnotation()) {
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
