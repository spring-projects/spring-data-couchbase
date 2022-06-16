/*
 * Copyright 2018-2022 the original author or authors
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

package org.springframework.data.couchbase.core.convert.join;

import static org.springframework.data.couchbase.core.query.N1QLExpression.i;
import static org.springframework.data.couchbase.core.query.N1QLExpression.x;
import static org.springframework.data.couchbase.core.support.TemplateUtils.SELECT_CAS;
import static org.springframework.data.couchbase.core.support.TemplateUtils.SELECT_ID;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.FetchType;
import org.springframework.data.couchbase.core.query.HashSide;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import org.springframework.data.couchbase.core.query.N1qlJoin;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.repository.Collection;
import org.springframework.data.couchbase.repository.Scope;
import org.springframework.data.couchbase.repository.query.StringBasedN1qlQueryParser;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.query.QueryOptions;

/**
 * N1qlJoinResolver resolves by converting the join definition to query statement and executing using CouchbaseTemplate
 *
 * @author Subhashni Balakrishnan
 * @author Michael Reiche
 */
public class N1qlJoinResolver {
	private static final Logger LOGGER = LoggerFactory.getLogger(N1qlJoinResolver.class);

	public static <L, R> String buildQuery(ReactiveCouchbaseTemplate template, String scope, String collection,
			N1qlJoinResolverParameters parameters) {
		String joinType = "JOIN";
		String selectEntity = "SELECT META(rks).id AS " + SELECT_ID + ", META(rks).cas AS " + SELECT_CAS + ", (rks).* ";

		StringBuilder useLKSBuilder = new StringBuilder();
		if (parameters.getJoinDefinition().index().length() > 0) {
			useLKSBuilder.append("INDEX(" + parameters.getJoinDefinition().index() + ")");
		}
		String useLKS = useLKSBuilder.length() > 0 ? "USE " + useLKSBuilder.toString() + " " : "";

		KeySpacePair keySpacePair = getKeySpacePair(template.getBucketName(), scope, collection, parameters);

		String from = "FROM " + keySpacePair.lhs.keyspace + " lks " + useLKS + joinType + " " + keySpacePair.rhs.keyspace
				+ " rks";

		StringBasedN1qlQueryParser.N1qlSpelValues n1qlL = Query.getN1qlSpelValues(template, null,
				keySpacePair.lhs.collection, parameters.getEntityTypeInfo().getType(), parameters.getEntityTypeInfo().getType(),
				false, null, null);
		String onLks = "lks." + n1qlL.filter;

		StringBasedN1qlQueryParser.N1qlSpelValues n1qlR = Query.getN1qlSpelValues(template, null,
				keySpacePair.rhs.collection, parameters.getAssociatedEntityTypeInfo().getType(),
				parameters.getAssociatedEntityTypeInfo().getType(), false, null, null);
		String onRks = "rks." + n1qlR.filter;

		StringBuilder useRKSBuilder = new StringBuilder();
		if (parameters.getJoinDefinition().rightIndex().length() > 0) {
			useRKSBuilder.append("INDEX(" + parameters.getJoinDefinition().rightIndex() + ")");
		}
		if (!parameters.getJoinDefinition().hashside().equals(HashSide.NONE)) {
			if (useRKSBuilder.length() > 0)
				useRKSBuilder.append(" ");
			useRKSBuilder.append("HASH(" + parameters.getJoinDefinition().hashside().getValue() + ")");
		}
		if (parameters.getJoinDefinition().keys().length > 0) {
			if (useRKSBuilder.length() > 0)
				useRKSBuilder.append(" ");
			useRKSBuilder.append("KEYS [");
			String[] keys = parameters.getJoinDefinition().keys();

			for (int i = 0; i < keys.length; i++) {
				if (i != 0)
					useRKSBuilder.append(",");
				useRKSBuilder.append("\"" + keys[i] + "\"");
			}
			useRKSBuilder.append("]");
		}

		String on = "ON " + parameters.getJoinDefinition().on().concat(" AND " + onLks).concat(" AND " + onRks);

		String where = "WHERE META(lks).id=\"" + parameters.getLksId() + "\"";
		where += ((parameters.getJoinDefinition().where().length() > 0) ? " AND " + parameters.getJoinDefinition().where()
				: "");

		StringBuilder statementSb = new StringBuilder();
		statementSb.append(selectEntity);
		statementSb.append(" " + from);
		statementSb.append((useRKSBuilder.length() > 0 ? " USE " + useRKSBuilder.toString() : ""));
		statementSb.append(" " + on);
		statementSb.append(" " + where);
		return statementSb.toString();
	}

	static KeySpacePair getKeySpacePair(String bucketName, String scope, String collection,
			N1qlJoinResolverParameters parameters) {
		Class<?> lhsClass = parameters.getEntityTypeInfo().getActualType().getType();
		String lhScope = scope != null ? scope : getScope(lhsClass);
		String lhCollection = collection != null ? collection : getCollection(lhsClass);
		Class<?> rhsClass = parameters.getAssociatedEntityTypeInfo().getActualType().getType();
		String rhScope = getScope(rhsClass);
		String rhCollection = getCollection(rhsClass);
		if (lhCollection != null && rhCollection != null) {
			// they both have non-default collections
			// It's possible that the scope for the lhs was set with an annotation on a repository method,
			// the entity class or the repository class or a query option. Since there is no means to set
			// the scope of the associated class by the method, repository class or query option (only
			// the annotation) we assume that the (possibly) dynamic scope of the entity would be a better
			// choice as it is logical to put collections to be joined in the same scope. Note that lhScope
			// is used for both keyspaces.
			return new KeySpacePair(lhCollection, x(i(bucketName) + "." + i(lhScope) + "." + i(lhCollection)), //
					rhCollection, x(i(bucketName) + "." + i(lhScope) + "." + i(rhCollection)));
		} else if (lhCollection != null && rhCollection == null) {
			// the lhs has a collection (and therefore a scope as well), but the rhs does not have a collection.
			// Use the lhScope and lhCollection for the entity. The rhs is just the bucket.
			return new KeySpacePair(lhCollection, x(i(bucketName) + "." + i(lhScope) + "." + i(lhCollection)), //
					null, i(bucketName));
		} else if (lhCollection != null && rhCollection == null) {
			// the lhs does not have a collection (or scope), but rhs does have a collection
			// Using the same (default) scope for the rhs would mean specifying a
			// non-default collection in a default scope - which is not allowed.
			// So use the scope and collection from the associated class.
			return new KeySpacePair(null, i(bucketName), //
					rhCollection, x(i(bucketName) + "." + i(rhScope) + "." + i(rhCollection)));
		} else { // neither have collections, just use the bucket.
			return new KeySpacePair(null, i(bucketName), null, i(bucketName));
		}
	}

	static class KeySpacePair {
		KeySpaceInfo lhs;
		KeySpaceInfo rhs;

		public KeySpacePair(String lhsCollection, N1QLExpression lhsKeyspace, String rhsCollection,
				N1QLExpression rhsKeyspace) {
			this.lhs = new KeySpaceInfo(lhsCollection, lhsKeyspace);
			this.rhs = new KeySpaceInfo(rhsCollection, rhsKeyspace);
		}

		static class KeySpaceInfo {
			String collection;
			N1QLExpression keyspace;

			public KeySpaceInfo(String collection, N1QLExpression keyspace) {
				this.collection = collection;
				this.keyspace = keyspace;
			}
		}
	}

	/**
	 * from CouchbaseQueryMethod.getCollection()
	 * 
	 * @param targetClass
	 * @return
	 */
	static String getCollection(Class<?> targetClass) {
		// Could try the repository method, then the targetClass, then the repository class, then the entity class
		// but we don't have the repository method nor the repositoryMetdata at this point.
		AnnotatedElement[] annotated = new AnnotatedElement[] { targetClass };
		return OptionsBuilder.annotationString(Collection.class, CollectionIdentifier.DEFAULT_COLLECTION, annotated);
	}

	/**
	 * from CouchbaseQueryMethod.getScope()
	 * 
	 * @param targetClass
	 * @return
	 */
	static String getScope(Class<?> targetClass) {
		// Could try the repository method, then the targetClass, then the repository class, then the entity class
		// but we don't have the repository method nor the repositoryMetdata at this point.
		AnnotatedElement[] annotated = new AnnotatedElement[] { targetClass };
		return OptionsBuilder.annotationString(Scope.class, CollectionIdentifier.DEFAULT_SCOPE, annotated);
	}

	public static <R> List<R> doResolve(ReactiveCouchbaseTemplate template, String scopeName, String collectionName,
			N1qlJoinResolverParameters parameters, Class<R> associatedEntityClass) {

		String statement = buildQuery(template, scopeName, collectionName, parameters);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Join query executed " + statement);
		}

		N1QLQuery query = new N1QLQuery(N1QLExpression.x(statement), QueryOptions.queryOptions());
		List<R> result = template.findByQuery(associatedEntityClass).matching(query).all().collectList().block();
		return result.isEmpty() ? null : result;
	}

	public static boolean isLazyJoin(N1qlJoin joinDefinition) {
		return joinDefinition.fetchType().equals(FetchType.LAZY);
	}

	public static void handleProperties(CouchbasePersistentEntity<?> persistentEntity,
			ConvertingPropertyAccessor<?> accessor, ReactiveCouchbaseTemplate template, String id, String scope,
			String collection) {
		persistentEntity.doWithProperties((PropertyHandler<CouchbasePersistentProperty>) prop -> {
			if (prop.isAnnotationPresent(N1qlJoin.class)) {
				N1qlJoin definition = prop.findAnnotation(N1qlJoin.class);
				TypeInformation type = prop.getTypeInformation().getActualType();
				Class clazz = type.getType();
				N1qlJoinResolver.N1qlJoinResolverParameters parameters = new N1qlJoinResolver.N1qlJoinResolverParameters(
						definition, id, persistentEntity.getTypeInformation(), type, scope, collection);
				if (N1qlJoinResolver.isLazyJoin(definition)) {
					N1qlJoinResolver.N1qlJoinProxy proxy = new N1qlJoinResolver.N1qlJoinProxy(template, parameters);
					accessor.setProperty(prop,
							java.lang.reflect.Proxy.newProxyInstance(List.class.getClassLoader(), new Class[] { List.class }, proxy));
				} else {
					// clazz needs to be passes instead of just using
					// parameters.associatedType.getTypeInformation().getActualType().getType
					// to keep the compiler happy for the call template.findByQuery(associatedEntityClass)
					accessor.setProperty(prop, N1qlJoinResolver.doResolve(template, scope, collection, parameters, clazz));
				}
			}
		});
	}

	static public class N1qlJoinProxy implements InvocationHandler {
		private final ReactiveCouchbaseTemplate reactiveTemplate;
		private final String collectionName = null;
		private final String scopeName = null;
		private final N1qlJoinResolverParameters params;
		private List<?> resolved = null;

		public N1qlJoinProxy(ReactiveCouchbaseTemplate template, N1qlJoinResolverParameters params) {
			this.reactiveTemplate = template;
			this.params = params;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			if (this.resolved == null) {
				this.resolved = doResolve(this.reactiveTemplate, this.params.getScopeName(), this.params.getCollectionName(),
						this.params, this.params.associatedEntityTypeInfo.getType());
			}
			return method.invoke(this.resolved, args);
		}
	}

	static public class N1qlJoinResolverParameters {
		private N1qlJoin joinDefinition;
		private String lksId;
		private TypeInformation<?> entityTypeInfo;
		private TypeInformation<?> associatedEntityTypeInfo;
		private String scopeName;
		private String collectionName;

		public N1qlJoinResolverParameters(N1qlJoin joinDefinition, String lksId, TypeInformation<?> entityTypeInfo,
				TypeInformation<?> associatedEntityTypeInfo, String scopeName, String collectionName) {
			Assert.notNull(joinDefinition, "The join definition is required");
			Assert.notNull(entityTypeInfo, "The entity type information is required");
			Assert.notNull(associatedEntityTypeInfo, "The associated entity type information is required");

			this.joinDefinition = joinDefinition;
			this.lksId = lksId;
			this.entityTypeInfo = entityTypeInfo;
			this.associatedEntityTypeInfo = associatedEntityTypeInfo;
			this.scopeName = scopeName;
			this.collectionName = collectionName;
		}

		public N1qlJoin getJoinDefinition() {
			return joinDefinition;
		}

		public String getLksId() {
			return lksId;
		}

		public TypeInformation getEntityTypeInfo() {
			return entityTypeInfo;
		}

		public TypeInformation getAssociatedEntityTypeInfo() {
			return associatedEntityTypeInfo;
		}

		public String getScopeName() {
			return scopeName;
		}

		public String getCollectionName() {
			return collectionName;
		}
	}
}
