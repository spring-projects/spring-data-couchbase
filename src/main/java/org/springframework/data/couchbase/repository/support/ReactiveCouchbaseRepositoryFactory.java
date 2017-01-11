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
package org.springframework.data.couchbase.repository.support;

import java.io.Serializable;
import java.lang.reflect.Method;

import com.couchbase.client.java.util.features.CouchbaseFeature;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.couchbase.core.RxJavaCouchbaseOperations;
import org.springframework.data.couchbase.core.UnsupportedCouchbaseFeatureException;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.*;
import org.springframework.data.couchbase.repository.config.ReactiveRepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.query.*;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.ReactiveRepositoryFactorySupport;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;

/**
 * @Subhashni Balakrishnan
 * @since 3.0
 */
public class ReactiveCouchbaseRepositoryFactory extends ReactiveRepositoryFactorySupport {
    private static final SpelExpressionParser SPEL_PARSER = new SpelExpressionParser();

    /**
     * Holds the reference to the template.
     */
    private final ReactiveRepositoryOperationsMapping couchbaseOperationsMapping;

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
    public ReactiveCouchbaseRepositoryFactory(final ReactiveRepositoryOperationsMapping couchbaseOperationsMapping,
                                              final IndexManager indexManager) {
        Assert.notNull(couchbaseOperationsMapping);
        Assert.notNull(indexManager);

        this.couchbaseOperationsMapping = couchbaseOperationsMapping;
        this.indexManager = indexManager;
        mappingContext = this.couchbaseOperationsMapping.getMappingContext();
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
     * Returns a new Repository based on the metadata. Two categories of repositories can be instantiated:
     * {@link SimpleReactiveCouchbaseRepository} and {@link ReactiveN1qlCouchbaseRepository}.
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
        RxJavaCouchbaseOperations couchbaseOperations = couchbaseOperationsMapping.resolve(metadata.getRepositoryInterface(),
                metadata.getDomainType());
        boolean isN1qlAvailable = couchbaseOperations.getCouchbaseClusterInfo().checkAvailable(CouchbaseFeature.N1QL);

        ViewIndexed viewIndexed = AnnotationUtils.findAnnotation(metadata.getRepositoryInterface(), ViewIndexed.class);
        N1qlPrimaryIndexed n1qlPrimaryIndexed = AnnotationUtils.findAnnotation(metadata.getRepositoryInterface(), N1qlPrimaryIndexed.class);
        N1qlSecondaryIndexed n1qlSecondaryIndexed = AnnotationUtils.findAnnotation(metadata.getRepositoryInterface(), N1qlSecondaryIndexed.class);

        checkFeatures(metadata, isN1qlAvailable, n1qlPrimaryIndexed, n1qlSecondaryIndexed);

        indexManager.buildIndexes(metadata, viewIndexed, n1qlPrimaryIndexed, n1qlSecondaryIndexed, couchbaseOperations);

        CouchbaseEntityInformation<?, Serializable> entityInformation = getEntityInformation(metadata.getDomainType());
        SimpleReactiveCouchbaseRepository repo = getTargetRepositoryViaReflection(metadata, entityInformation, couchbaseOperations);
        repo.setViewMetadataProvider(viewPostProcessor.getViewMetadataProvider());
        return repo;
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
     * Returns the base class for the repository being constructed. Two categories of repositories can be produced by
     * this factory: {@link SimpleReactiveCouchbaseRepository} and {@link ReactiveN1qlCouchbaseRepository}. This method checks if N1QL
     * is available to choose between the two, but the actual concrete class is determined respectively by
     * {@link #getSimpleBaseClass(RepositoryMetadata)} and {@link #getN1qlBaseClass(RepositoryMetadata)}.
     *
     * Override these methods if you want to change the base class for all your repositories.
     *
     * @param repositoryMetadata metadata for the repository.
     *
     * @return the base class.
     */
    @Override
    protected final Class<?> getRepositoryBaseClass(final RepositoryMetadata repositoryMetadata) {
        RxJavaCouchbaseOperations couchbaseOperations = couchbaseOperationsMapping.resolve(repositoryMetadata.getRepositoryInterface(),
                repositoryMetadata.getDomainType());
        boolean isN1qlAvailable = couchbaseOperations.getCouchbaseClusterInfo().checkAvailable(CouchbaseFeature.N1QL);
        if (isN1qlAvailable) {
            return getN1qlBaseClass(repositoryMetadata);
        }
        return getSimpleBaseClass(repositoryMetadata);
    }

    protected Class<? extends ReactiveN1qlCouchbaseRepository> getN1qlBaseClass(final RepositoryMetadata repositoryMetadata) {
        return ReactiveN1qlCouchbaseRepository.class;
    }

    protected Class<? extends SimpleReactiveCouchbaseRepository> getSimpleBaseClass(final RepositoryMetadata repositoryMetadata) {
        return SimpleReactiveCouchbaseRepository.class;
    }

    @Override
    protected QueryLookupStrategy getQueryLookupStrategy(QueryLookupStrategy.Key key, EvaluationContextProvider contextProvider) {
        return new ReactiveCouchbaseRepositoryFactory.CouchbaseQueryLookupStrategy(contextProvider);
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
        public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory, NamedQueries namedQueries) {
            RxJavaCouchbaseOperations couchbaseOperations = couchbaseOperationsMapping.resolve(metadata.getRepositoryInterface(),
                    metadata.getDomainType());

            CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, factory, mappingContext);
            String namedQueryName = queryMethod.getNamedQueryName();

            if (queryMethod.hasDimensionalAnnotation()) {
                return new ReactiveSpatialViewBasedQuery(queryMethod, couchbaseOperations);
            } else if (queryMethod.hasViewAnnotation()) {
                return new ReactiveViewBasedCouchbaseQuery(queryMethod, couchbaseOperations);
            } else if (queryMethod.hasN1qlAnnotation()) {
                if (queryMethod.hasInlineN1qlQuery()) {
                    return new ReactiveStringN1qlBasedQuery(queryMethod.getInlineN1qlQuery(), queryMethod, couchbaseOperations,
                            SPEL_PARSER, evaluationContextProvider);
                } else if (namedQueries.hasQuery(namedQueryName)) {
                    String namedQuery = namedQueries.getQuery(namedQueryName);
                    return new ReactiveStringN1qlBasedQuery(namedQuery, queryMethod, couchbaseOperations,
                            SPEL_PARSER, evaluationContextProvider);
                } //otherwise will do default, queryDerivation
            }
            return new ReactivePartTreeN1qlBasedQuery(queryMethod, couchbaseOperations);
        }
    }


}
