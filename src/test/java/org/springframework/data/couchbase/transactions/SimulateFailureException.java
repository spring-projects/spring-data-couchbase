package org.springframework.data.couchbase.transactions;

class SimulateFailureException extends RuntimeException {

  public SimulateFailureException(String... s){
    super(s!= null && s.length > 0 ? s[0] : null);
  }

  public SimulateFailureException(){}

  public static void throwEx(String... s){
    throw new SimulateFailureException(s);
  }

}
