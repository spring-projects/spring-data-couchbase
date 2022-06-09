package org.springframework.data.couchbase.transactions;

import org.junit.Assert;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.util.JavaIntegrationTests;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

class ReplaceLoopThread extends Thread {
  private final CouchbaseOperations couchbaseOperations;
  AtomicBoolean stop = new AtomicBoolean(false);
  UUID id;
  int maxIterations = 100;

  public ReplaceLoopThread(CouchbaseOperations couchbaseOperations, UUID id, int... iterations) {
    Assert.assertNotNull("couchbaseOperations cannot be null", couchbaseOperations);
    this.couchbaseOperations = couchbaseOperations;
    this.id = id;
    if (iterations != null && iterations.length == 1) {
      this.maxIterations = iterations[0];
    }
  }

  public void run() {
    for (int i = 0; i < maxIterations && !stop.get(); i++) {
      JavaIntegrationTests.sleepMs(10);
      try {
        // note that this does not go through spring-data, therefore it does not have the @Field , @Version etc.
        // annotations processed so we just check getFirstname().equals()
        // switchedPerson has version=0, so it doesn't check CAS
        Person fetched = couchbaseOperations.findById(Person.class).one(id.toString());
        couchbaseOperations.replaceById(Person.class).one(fetched.withFirstName("Changed externally"));
        System.out.println("********** replace thread: " + i + " success");
      } catch (Exception e) {
        System.out.println("********** replace thread: " + i + " " + e.getClass()
            .getName());
        e.printStackTrace();
      }
    }

  }

  public void setStopFlag() {
    stop.set(true);
  }

  static Person updateOutOfTransaction(CouchbaseOperations couchbaseOperations, Person pp, int tryCount) {
    System.err.println("updateOutOfTransaction: "+tryCount);
    if (tryCount < 1) {
      throw new RuntimeException("increment before calling updateOutOfTransactions");
    }
    if (tryCount > 1) {
      return pp;
    }
    ReplaceLoopThread t = new ReplaceLoopThread(couchbaseOperations,
        pp.getId(), 1);
    t.start();
    try {
      t.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return pp;
  }
}
