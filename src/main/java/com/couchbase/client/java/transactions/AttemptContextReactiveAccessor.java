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

import java.lang.reflect.Field;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.java.codec.JsonSerializer;

/**
 * To access the ReactiveTransactionAttemptContext held by TransactionAttemptContext
 *
 * @author Michael Reiche
 */
@Stability.Internal
public class AttemptContextReactiveAccessor {
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
}