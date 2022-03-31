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
package com.querydsl.couchbase.document;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.Nullable;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.query.QueryCriteriaDefinition;

import com.querydsl.core.DefaultQueryMetadata;
import com.querydsl.core.JoinExpression;
import com.querydsl.core.QueryMetadata;
import com.querydsl.core.QueryModifiers;
import com.querydsl.core.SimpleQuery;
import com.querydsl.core.support.QueryMixin;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.FactoryExpression;
import com.querydsl.core.types.Operation;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.ParamExpression;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.Predicate;

/**
 * renamed from AbstractCouchbaseQuery to AbstractCouchbaseQueryDSL to avoid confusion with the AbstractCouchbaseQuery
 * that is in the package com.querydsl.couchbase
 *
 * @author Michael Reiche
 */

public abstract class AbstractCouchbaseQueryDSL<Q extends AbstractCouchbaseQueryDSL<Q>> implements SimpleQuery<Q> {
	private final CouchbaseDocumentSerializer serializer;
	private final QueryMixin<Q> queryMixin;// = new QueryMixin(this, new DefaultQueryMetadata(), false);
	// TODO private ReadPreference readPreference;

	public AbstractCouchbaseQueryDSL(CouchbaseDocumentSerializer serializer) {
		this.serializer = serializer;
		@SuppressWarnings("unchecked") // Q is this plus subclass
		Q query = (Q) this;
		this.queryMixin = new QueryMixin<Q>(query, new DefaultQueryMetadata(), false);
	}

	/**
	 * mongodb uses createQuery(Predicate filter) where the serializer creates the 'query' <br>
	 * and then uses the result to create a BasicQuery with queryObject = result <br>
	 * Couchbase Query has a 'criteria' which is a <br>
	 * List<QueryCriteriaDefinition> criteria <br>
	 * so we could create a List&lt;QueryCriteriaDefinition&gt; or an uber QueryCriteria that combines <br>
	 * all the sub QueryDefinitions in the filter.
	 */
	protected QueryCriteriaDefinition createCriteria(Predicate predicate) {
		// mongodb uses createQuery(Predicate filter) where the serializer creates the 'queryObject' of the BasicQuery
		return predicate != null ? (QueryCriteriaDefinition) this.serializer.handle(predicate) : null;
	}

	// TODO - need later
	// public <T> JoinBuilder<Q, T> join(Path<T> ref, Path<T> target) {
	// return new JoinBuilder(this.queryMixin, ref, target);
	// }

	// public <T> JoinBuilder<Q, T> join(CollectionPathBase<?, T, ?> ref, Path<T> target) {
	// return new JoinBuilder(this.queryMixin, ref, target);
	// }

	// public <T> AnyEmbeddedBuilder<Q> anyEmbedded(Path<? extends Collection<T>> collection, Path<T> target) {
	// return new AnyEmbeddedBuilder(this.queryMixin, collection);
	// }

	@Nullable
	protected Predicate createFilter(QueryMetadata metadata) {
		Predicate filter;
		if (!metadata.getJoins().isEmpty()) {
			filter = ExpressionUtils.allOf(new Predicate[] { metadata.getWhere(), this.createJoinFilter(metadata) });
		} else {
			filter = metadata.getWhere();
		}
		return filter;
	}

	@Nullable
	protected Predicate createJoinFilter(QueryMetadata metadata) {
		Map<Expression<?>, Predicate> predicates = new HashMap();
		List<JoinExpression> joins = metadata.getJoins();

		for (int i = joins.size() - 1; i >= 0; --i) {
			JoinExpression join = (JoinExpression) joins.get(i);
			Path<?> source = (Path) ((Operation) join.getTarget()).getArg(0);
			Path<?> target = (Path) ((Operation) join.getTarget()).getArg(1);
			Predicate extraFilters = (Predicate) predicates.get(target.getRoot());
			Predicate filter = ExpressionUtils.allOf(new Predicate[] { join.getCondition(), extraFilters });
			List<? extends Object> ids = this.getIds(target.getType(), filter);
			if (ids.isEmpty()) {
				throw new AbstractCouchbaseQueryDSL.NoResults();
			}

			Path<?> path = ExpressionUtils.path(String.class, source, "$id");
			predicates.merge(source.getRoot(),
					ExpressionUtils.in(path, (Collection) ids/* TODO was just ids without casting to Collection */),
					ExpressionUtils::and);
		}

		Path<?> source = (Path) ((Operation) ((JoinExpression) joins.get(0)).getTarget()).getArg(0);
		return predicates.get(source.getRoot());
	}

	private Predicate allOf(Collection<Predicate> predicates) {
		return predicates != null ? ExpressionUtils.allOf(predicates) : null;
	}

	protected abstract List<Object> getIds(Class<?> var1, Predicate var2);

	public Q distinct() {
		return this.queryMixin.distinct();
	}

	public Q where(Predicate e) {
		return this.queryMixin.where(e);
	}

	public Q where(Predicate... e) {
		return this.queryMixin.where(e);
	}

	public Q limit(long limit) {
		return this.queryMixin.limit(limit);
	}

	public Q offset(long offset) {
		return this.queryMixin.offset(offset);
	}

	public Q restrict(QueryModifiers modifiers) {
		return this.queryMixin.restrict(modifiers);
	}

	public Q orderBy(OrderSpecifier<?> o) {
		return this.queryMixin.orderBy(o);
	}

	public Q orderBy(OrderSpecifier<?>... o) {
		return this.queryMixin.orderBy(o);
	}

	public <T> Q set(ParamExpression<T> param, T value) {
		return this.queryMixin.set(param, value);
	}

	protected Map<String, String> createProjection(Expression<?> projection) {
		if (projection instanceof FactoryExpression) {
			Map<String, String> obj = new HashMap();
			Iterator var3 = ((FactoryExpression) projection).getArgs().iterator();

			while (var3.hasNext()) {
				Object expr = var3.next();
				if (expr instanceof Expression) {
					obj.put(expr.toString(), (String) this.serializer.handle((Expression) expr));
				}
			}
			return obj;
		} else {
			return null;
		}
	}

	protected CouchbaseDocument createQuery(@Nullable Predicate predicate) {
		return predicate != null ? (CouchbaseDocument) this.serializer.handle(predicate) : new CouchbaseDocument();
	}

	// public void setReadPreference(ReadPreference readPreference) {
	// this.readPreference = readPreference;
	// }

	protected QueryMixin<Q> getQueryMixin() {
		return this.queryMixin;
	}

	protected CouchbaseDocumentSerializer getSerializer() {
		return this.serializer;
	}

	// protected ReadPreference getReadPreference() {
	// return this.readPreference;
	// }

	public CouchbaseDocument asDocument() {
		return this.createQuery(this.queryMixin.getMetadata().getWhere());
	}

	public String toString() {
		return this.asDocument().toString();
	}

	static class NoResults extends RuntimeException {
		NoResults() {}
	}
}
