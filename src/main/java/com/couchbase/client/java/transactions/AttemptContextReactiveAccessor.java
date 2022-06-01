/*
/*
 * Copyright 2021 the original author or authors
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
package com.couchbase.client.java.transactions;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.logging.Logger;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.EncodingFailureException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.core.transaction.CoreTransactionContext;
import com.couchbase.client.core.transaction.CoreTransactionsReactive;
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig;
import com.couchbase.client.core.transaction.config.CoreTransactionOptions;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.core.transaction.support.AttemptState;
import com.couchbase.client.java.ReactiveScope;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

/**
 * To access the ReactiveTransactionAttemptContext held by TransactionAttemptContext
 *
 * @author Michael Reiche
 */
public class AttemptContextReactiveAccessor {

	public static ReactiveTransactions reactive(Transactions transactions) {
		try {
			Field field = Transactions.class.getDeclaredField("reactive");
			field.setAccessible(true);
			return (ReactiveTransactions) field.get(transactions);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}
	}

	public static ReactiveTransactionAttemptContext reactive(TransactionAttemptContext atr) {
		JsonSerializer serializer;
		try {
			Field field = TransactionAttemptContext.class.getDeclaredField("serializer");
			field.setAccessible(true);
			serializer = (JsonSerializer) field.get(atr);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}
		return new ReactiveTransactionAttemptContext(getCore(atr), serializer);
	}

	public static CoreTransactionLogger getLogger(TransactionAttemptContext attemptContextReactive) {
		return attemptContextReactive.logger();
	}

	// todo gp needed?
	@Stability.Internal
	public static CoreTransactionAttemptContext newCoreTranactionAttemptContext(ReactiveTransactions transactions) {

		String txnId = UUID.randomUUID().toString();
		CoreTransactionsReactive coreTransactionsReactive;
		try {
			Field field = ReactiveTransactions.class.getDeclaredField("internal");
			field.setAccessible(true);
			coreTransactionsReactive = (CoreTransactionsReactive) field.get(transactions);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}

		// todo gpx options need to be loaded from Cluster and TransactionOptions (from TransactionsWrapper)
		CoreTransactionOptions perConfig = new CoreTransactionOptions(Optional.empty(), Optional.empty(), Optional.empty(),
				Optional.of(Duration.ofMinutes(10)), Optional.empty(), Optional.empty());

		CoreMergedTransactionConfig merged = new CoreMergedTransactionConfig(coreTransactionsReactive.config(),
				Optional.ofNullable(perConfig));
		CoreTransactionContext overall = new CoreTransactionContext(
				coreTransactionsReactive.core().context().environment().requestTracer(),
				coreTransactionsReactive.core().context().environment().eventBus(), UUID.randomUUID().toString(), merged,
				coreTransactionsReactive.core().transactionsCleanup());

		CoreTransactionAttemptContext coreTransactionAttemptContext = coreTransactionsReactive.createAttemptContext(overall,
				merged, txnId);
		return coreTransactionAttemptContext;
	}

	public static CoreTransactionAttemptContext getCore(ReactiveTransactionAttemptContext atr) {
		CoreTransactionAttemptContext coreTransactionsReactive;
		try {
			Field field = ReactiveTransactionAttemptContext.class.getDeclaredField("internal");
			field.setAccessible(true);
			coreTransactionsReactive = (CoreTransactionAttemptContext) field.get(atr);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}
		return coreTransactionsReactive;
	}

	public static CoreTransactionAttemptContext getCore(TransactionAttemptContext atr) {
		try {
			Field field = TransactionAttemptContext.class.getDeclaredField("internal");
			field.setAccessible(true);
			return (CoreTransactionAttemptContext) field.get(atr);
		} catch (Throwable err) {
			throw new RuntimeException(err);
		}
	}

	public static ReactiveTransactionAttemptContext createReactiveTransactionAttemptContext(
			CoreTransactionAttemptContext core, JsonSerializer jsonSerializer) {
		return new ReactiveTransactionAttemptContext(core, jsonSerializer);
	}

	public static TransactionResult run(Transactions transactions, Consumer<TransactionAttemptContext> transactionLogic, CoreTransactionOptions coreTransactionOptions) {
		return reactive(transactions).runBlocking(transactionLogic, coreTransactionOptions);
	}

	// todo gp have C&Ped this from SDK, needs to be exposed properly in SDK
	public static ObjectNode createTransactionOptions(final ReactiveScope scope,
																				  final String statement,
																				  final TransactionQueryOptions options) {
		JsonObject json = JsonObject.create()
				.put("statement", statement);
		if (scope != null) {
			json.put("query_context", QueryRequest.queryContext(scope.bucketName(), scope.name()));
		}
		if (options != null) {
			options.builder().build().injectParams(json);
		}
		try {
			ObjectNode opts = Mapper.reader().readValue(json.toBytes(), ObjectNode.class);
			return opts;
		} catch (IOException e) {
			throw new EncodingFailureException(e);
		}
	}

}
