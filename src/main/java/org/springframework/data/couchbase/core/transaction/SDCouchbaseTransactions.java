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
package org.springframework.data.couchbase.core.transaction;

import com.couchbase.transactions.AttemptContextReactive;
import com.couchbase.transactions.TransactionGetResult;
import com.couchbase.transactions.TransactionResult;
import com.couchbase.transactions.Transactions;
import com.couchbase.transactions.TransactionsReactive;
import com.couchbase.transactions.config.PerTransactionConfigBuilder;
import org.springframework.data.couchbase.repository.support.TransactionResultHolder;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class SDCouchbaseTransactions {

  Transactions transactions;
  Map<Integer, TransactionResultHolder> getResultMap = new HashMap<>();
  private AttemptContextReactive ctx;

  public SDCouchbaseTransactions(Transactions transactions) {
    this.transactions = transactions;
  }

  public TransactionsReactive reactive(){
    return transactions.reactive();
  }

  public AttemptContextReactive getCtx(){
    return ctx;
  }

 // public Mono<TransactionResult> reactive(Function<AttemptContextReactive, Mono<Void>> transactionLogic) {
 //   return reactive(transactionLogic, true);
 // }
  /**
   * A convenience wrapper around {@link TransactionsReactive#run}, that provides a default <code>PerTransactionConfig</code>.
   */
  public Mono<TransactionResult> reactive(Function<AttemptContextReactive, Mono<Void>> transactionLogic/*, boolean commit*/) {
    return transactions.reactive((ctx) -> {
      setAttemptTransactionReactive(ctx);
      return transactionLogic.apply(ctx); }, PerTransactionConfigBuilder.create().build()/*, commit*/);

  }

  public TransactionResultHolder transactionGetResult(Integer key){
    return getResultMap.get(key);
  }

  public TransactionResultHolder transactionGetResult(TransactionGetResult result){
    TransactionResultHolder holder = new TransactionResultHolder(result);
    getResultMap.put(System.identityHashCode(holder), holder);
    return holder;
  }

  public void setAttemptTransactionReactive(AttemptContextReactive ctx) {
    this.ctx = ctx;
  }

}
