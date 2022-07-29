/*
 * Copyright 2022 the original author or authors
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
package com.example.demo;

import com.couchbase.client.java.transactions.TransactionResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.TransactionalSupport;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

/**
 *
 * 
 * @author Michael Reiche
 */
@Service
@Transactional
public class AirportService {

  public AirportService(CouchbaseTemplate template){
    this.template = template;
  }

  CouchbaseTemplate template;
  public void transferGates(String fromId, String toId, int gatesToTransfer) {
    TransactionalSupport.checkForTransactionInThreadLocalStorage()
        .map( (h) -> { if ( ! h.isPresent() ) throw new RuntimeException("not in transaction!"); return h; } );

    TransactionalSupport.checkForTransactionInThreadLocalStorage().map(stat -> {
          Assert.isTrue(stat.isPresent(), "Not in transaction");
          return stat;
        });
      Airport fromAirport = template.findById(Airport.class).one(fromId);
      Airport toAirport = template.findById(Airport.class).one(toId);
      toAirport.gates += gatesToTransfer;
      fromAirport.gates -= gatesToTransfer;
      template.save(fromAirport);
      template.save(toAirport);
  }
}
