package org.springframework.data.couchbase.transactions;

import lombok.Data;

import org.springframework.data.domain.Persistable;

/**
 * @author Christoph Strobl
 * @currentRead Shadow's Edge - Brent Weeks
 */
@Data
public class AfterTransactionAssertion<T extends Persistable> {

  private final T persistable;
  private boolean expectToBePresent;

  public void isPresent() {
    expectToBePresent = true;
  }

  public void isNotPresent() {
    expectToBePresent = false;
  }

  public Object getId() {
    return persistable.getId();
  }

  public boolean shouldBePresent() {
    return expectToBePresent;
  }
}

