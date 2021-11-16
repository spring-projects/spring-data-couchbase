package org.springframework.data.couchbase.transaction;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.Semaphore;

class AbortCommitSubscriber<T> implements Subscriber<T> {
  private Subscription subscription;
  private final String name;
  private final Semaphore lock;

  public AbortCommitSubscriber(String name){
    this.name = name;
    this.lock = new Semaphore(1);
    try {
      lock.acquire();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  /**
   * This method is triggered when the Subscriber subscribes to a Publisher
   */
  @Override
  public void onSubscribe(Subscription subscription) {
    this.subscription = subscription;
    subscription.request(1);
  }

  /**
   * This method is triggered the Subscriber receives an event
   * signaling an item being sent from the publisher. The Item is simply printed here.
   */
  @Override
  public void onNext(T item) {
    subscription.request(1);
  }
  /**
   * This method is triggered when the Subscriber receives an error event.
   * In our case we just print the error message.
   */
  @Override
  public void onError(Throwable error) {
    System.err.println(name + " Error Occurred: " + error.getMessage());
  }
  /**
   * This method is triggered when the Subscriber Receives a complete. This means
   * it has already received and processed all items from the publisher to which it is subscribed.
   */
  @Override
  public void onComplete() {
    lock.release();
  }

  public Semaphore getLock() {
    return lock;
  }

  public void waitUntilComplete() {
    try {
      lock.acquire(1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
