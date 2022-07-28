package com.example.demo;

import com.couchbase.client.java.transactions.TransactionResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.TransactionalSupport;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

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
