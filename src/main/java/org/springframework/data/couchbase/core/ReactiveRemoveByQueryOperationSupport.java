/*
 * Copyright 2012-2021 the original author or authors
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
package org.springframework.data.couchbase.core;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.transactions.AttemptContextReactiveAccessor;
import com.couchbase.client.java.transactions.TransactionQueryOptions;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionalOperator;
import reactor.core.publisher.Flux;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.data.couchbase.core.support.TemplateUtils;
import org.springframework.util.Assert;

import com.couchbase.client.java.ReactiveScope;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.query.ReactiveQueryResult;

public class ReactiveRemoveByQueryOperationSupport implements ReactiveRemoveByQueryOperation {

	private static final Query ALL_QUERY = new Query();

	private final ReactiveCouchbaseTemplate template;
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveRemoveByQueryOperationSupport.class);

	public ReactiveRemoveByQueryOperationSupport(final ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveRemoveByQuery<T> removeByQuery(Class<T> domainType) {
		return new ReactiveRemoveByQuerySupport<>(template, domainType, ALL_QUERY, null, null, null, null, null);
	}

	static class ReactiveRemoveByQuerySupport<T> implements ReactiveRemoveByQuery<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<T> domainType;
		private final Query query;
		private final QueryScanConsistency scanConsistency;
		private final String scope;
		private final String collection;
		private final QueryOptions options;
		private final CouchbaseTransactionalOperator txCtx;

		ReactiveRemoveByQuerySupport(final ReactiveCouchbaseTemplate template, final Class<T> domainType, final Query query,
									 final QueryScanConsistency scanConsistency, String scope, String collection, QueryOptions options,
									 CouchbaseTransactionalOperator txCtx) {
			this.template = template;
			this.domainType = domainType;
			this.query = query;
			this.scanConsistency = scanConsistency;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.txCtx = txCtx;
		}

		@Override
		public Flux<RemoveResult> all() {
			PseudoArgs<QueryOptions> pArgs = new PseudoArgs<>(template, scope, collection, options, txCtx, domainType);
			String statement = assembleDeleteQuery(pArgs.getCollection());
			LOG.trace("removeByQuery {} statement: {}", pArgs, statement);
			ReactiveCouchbaseClientFactory clientFactory = template.getCouchbaseClientFactory();
			ReactiveScope rs = clientFactory.getScope(pArgs.getScope()).reactive();

			return TransactionalSupport.checkForTransactionInThreadLocalStorage(txCtx)
					.flatMapMany(transactionContext -> {

						if (!transactionContext.isPresent()) {
							QueryOptions opts = buildQueryOptions(pArgs.getOptions());
							return (pArgs.getScope() == null ? clientFactory.getCluster().reactive().query(statement, opts)
									: rs.query(statement, opts)).flatMapMany(ReactiveQueryResult::rowsAsObject)
									.map(row -> new RemoveResult(row.getString(TemplateUtils.SELECT_ID),
											row.getLong(TemplateUtils.SELECT_CAS), Optional.empty()));
						} else {
							TransactionQueryOptions opts = OptionsBuilder.buildTransactionQueryOptions(buildQueryOptions(pArgs.getOptions()));
							ObjectNode convertedOptions = AttemptContextReactiveAccessor.createTransactionOptions(pArgs.getScope() == null ? null : rs, statement, opts);
							return transactionContext.get().getCore().queryBlocking(statement, template.getBucketName(), pArgs.getScope(), convertedOptions, false)
									.flatMapIterable(result -> result.rows).map(row -> {
										JsonObject json = JsonObject.fromJson(row.data());
										return new RemoveResult(json.getString(TemplateUtils.SELECT_ID),
												json.getLong(TemplateUtils.SELECT_CAS), Optional.empty());
									});
						}
					});
		}

		private QueryOptions buildQueryOptions(QueryOptions options) {
			QueryScanConsistency qsc = scanConsistency != null ? scanConsistency : template.getConsistency();
			return query.buildQueryOptions(options, qsc);
		}

		@Override
		public RemoveByQueryTxOrNot<T> matching(final Query query) {
			return new ReactiveRemoveByQuerySupport<>(template, domainType, query, scanConsistency, scope, collection,
					options, txCtx);
		}

		@Override
		public RemoveByQueryWithQuery<T> inCollection(final String collection) {
			return new ReactiveRemoveByQuerySupport<>(template, domainType, query, scanConsistency, scope, collection,
					options, txCtx);
		}

		@Override
		@Deprecated
		public RemoveByQueryInScope<T> consistentWith(final QueryScanConsistency scanConsistency) {
			return new ReactiveRemoveByQuerySupport<>(template, domainType, query, scanConsistency, scope, collection,
					options, txCtx);
		}

		@Override
		public RemoveByQueryConsistentWith<T> withConsistency(final QueryScanConsistency scanConsistency) {
			return new ReactiveRemoveByQuerySupport<>(template, domainType, query, scanConsistency, scope, collection,
					options, txCtx);
		}

		private String assembleDeleteQuery(String collection) {
			return query.toN1qlRemoveString(template, collection, this.domainType);
		}

		@Override
		public RemoveByQueryWithQuery<T> withOptions(final QueryOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ReactiveRemoveByQuerySupport<>(template, domainType, query, scanConsistency, scope, collection,
					options, txCtx);
		}

		@Override
		public RemoveByQueryInCollection<T> inScope(final String scope) {
			return new ReactiveRemoveByQuerySupport<>(template, domainType, query, scanConsistency, scope, collection,
					options, txCtx);
		}

		@Override
		public RemoveByQueryWithConsistency<T> transaction(final CouchbaseTransactionalOperator txCtx) {
			return new ReactiveRemoveByQuerySupport<>(template, domainType, query, scanConsistency, scope, collection,
					options, txCtx);
		}

	}

}
