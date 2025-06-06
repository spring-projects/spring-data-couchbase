/*
 * Copyright 2013-2025 the original author or authors.
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

import static org.springframework.data.querydsl.QuerydslUtils.*;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Optional;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.query.CouchbaseQueryMethod;
import org.springframework.data.couchbase.repository.query.PartTreeCouchbaseQuery;
import org.springframework.data.couchbase.repository.query.StringBasedCouchbaseQuery;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryComposition;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.CachingValueExpressionDelegate;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;

/**
 * Factory to create {@link SimpleCouchbaseRepository} instances.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Michael Reiche
 */
public class CouchbaseRepositoryFactory extends RepositoryFactorySupport {

	private static final SpelExpressionParser SPEL_PARSER = new SpelExpressionParser();

	private final CouchbaseOperations operations;

	/**
	 * Holds the reference to the template.
	 */
	private final RepositoryOperationsMapping couchbaseOperationsMapping;

	/**
	 * Holds the mapping context.
	 */
	private final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;

	private final CrudMethodMetadataPostProcessor crudMethodMetadataPostProcessor;

	/**
	 * Create a new factory.
	 *
	 * @param couchbaseOperationsMapping the template for the underlying actions.
	 */
	public CouchbaseRepositoryFactory(final RepositoryOperationsMapping couchbaseOperationsMapping) {
		Assert.notNull(couchbaseOperationsMapping, "RepositoryOperationsMapping must not be null!");

		this.couchbaseOperationsMapping = couchbaseOperationsMapping;
		this.crudMethodMetadataPostProcessor = new CrudMethodMetadataPostProcessor();
		mappingContext = this.couchbaseOperationsMapping.getMappingContext();
		operations = this.couchbaseOperationsMapping.getDefault();
		addRepositoryProxyPostProcessor(crudMethodMetadataPostProcessor);
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		super.setBeanClassLoader(classLoader);
		this.crudMethodMetadataPostProcessor.setBeanClassLoader(classLoader);
	}

	/**
	 * Returns entity information based on the domain class.
	 *
	 * @param domainClass the class for the entity.
	 * @param <T> the value type
	 * @param <ID> the id type.
	 * @return entity information for that domain class.
	 */
	@Override
	public <T, ID> CouchbaseEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
		CouchbasePersistentEntity<T> entity = (CouchbasePersistentEntity<T>) mappingContext
				.getRequiredPersistentEntity(domainClass);
		return new MappingCouchbaseEntityInformation<>(entity);
	}

	/**
	 * Returns a new Repository based on the metadata. Two categories of repositories can be instantiated:
	 * {@link SimpleCouchbaseRepository}. This method performs feature checks to decide which of the two categories can be
	 * instantiated (eg. is N1QL available?). Instantiation is done via reflection, see
	 * {@link #getRepositoryBaseClass(RepositoryMetadata)}.
	 *
	 * @param metadata the repository metadata.
	 * @return a new created repository.
	 */
	@Override
	protected Object getTargetRepository(final RepositoryInformation metadata) {
		CouchbaseOperations couchbaseOperations = couchbaseOperationsMapping.resolve(metadata.getRepositoryInterface(),
				metadata.getDomainType());
		CouchbaseEntityInformation<?, Serializable> entityInformation = getEntityInformation(metadata.getDomainType());
		SimpleCouchbaseRepository repository = getTargetRepositoryViaReflection(metadata, entityInformation,
				couchbaseOperations, metadata.getRepositoryInterface());
		repository.setRepositoryMethodMetadata(crudMethodMetadataPostProcessor.getCrudMethodMetadata());
		return repository;
	}

	/**
	 * Returns the base class for the repository being constructed. We always return Override these methods if you want to
	 * change the base class for all your repositories.
	 *
	 * @param repositoryMetadata metadata for the repository.
	 * @return the base class.
	 */
	@Override
	protected final Class<?> getRepositoryBaseClass(final RepositoryMetadata repositoryMetadata) {
		return SimpleCouchbaseRepository.class;
	}

	@Override
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(QueryLookupStrategy.Key key,
			ValueExpressionDelegate valueExpressionDelegate) {
		return Optional.of(new CouchbaseQueryLookupStrategy(valueExpressionDelegate));
	}

	/**
	 * Strategy to lookup Couchbase queries implementation to be used.
	 */
	private class CouchbaseQueryLookupStrategy implements QueryLookupStrategy {

		private final ValueExpressionDelegate valueExpressionDelegate;

		public CouchbaseQueryLookupStrategy(ValueExpressionDelegate valueExpressionDelegate) {
			this.valueExpressionDelegate = new CachingValueExpressionDelegate(valueExpressionDelegate);
		}

		@Override
		public RepositoryQuery resolveQuery(final Method method, final RepositoryMetadata metadata,
				final ProjectionFactory factory, final NamedQueries namedQueries) {
			final CouchbaseOperations couchbaseOperations = couchbaseOperationsMapping
					.resolve(metadata.getRepositoryInterface(), metadata.getDomainType());

			CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, factory, mappingContext);

			if (queryMethod.hasN1qlAnnotation()) {
				return new StringBasedCouchbaseQuery(queryMethod, couchbaseOperations,
						valueExpressionDelegate, namedQueries);
			} else {
				return new PartTreeCouchbaseQuery(queryMethod, couchbaseOperations,
						valueExpressionDelegate);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getRepositoryFragments(org.springframework.data.repository.core.RepositoryMetadata)
	 */
	@Override
	protected RepositoryComposition.RepositoryFragments getRepositoryFragments(RepositoryMetadata metadata) {
		return getRepositoryFragments(metadata, operations);
	}

	/**
	 * Creates {@link RepositoryComposition.RepositoryFragments} based on {@link RepositoryMetadata} to add
	 * Couchbase-specific extensions. Typically adds a {@link QuerydslCouchbasePredicateExecutor} if the repository
	 * interface uses Querydsl.
	 * <p>
	 * Can be overridden by subclasses to customize {@link RepositoryComposition.RepositoryFragments}.
	 *
	 * @param metadata repository metadata.
	 * @param operations the Couchbase operations manager.
	 * @return
	 */
	protected RepositoryComposition.RepositoryFragments getRepositoryFragments(RepositoryMetadata metadata,
			CouchbaseOperations operations) {

		boolean isQueryDslRepository = QUERY_DSL_PRESENT
				&& QuerydslPredicateExecutor.class.isAssignableFrom(metadata.getRepositoryInterface());

		if (isQueryDslRepository) {

			if (metadata.isReactiveRepository()) {
				throw new InvalidDataAccessApiUsageException(
						"Cannot combine Querydsl and reactive repository support in a single interface");
			}

			return RepositoryComposition.RepositoryFragments
					.just(new QuerydslCouchbasePredicateExecutor<>(getEntityInformation(metadata.getDomainType()), operations));
		}

		return RepositoryComposition.RepositoryFragments.empty();
	}

}
