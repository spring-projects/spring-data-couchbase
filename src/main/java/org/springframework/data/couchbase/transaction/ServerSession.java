package org.springframework.data.couchbase.transaction;

/**
 * used only by ClientSession.getServerSession() - which returns null
 */

public interface ServerSession {
  String getIdentifier();

  long getTransactionNumber();

  long advanceTransactionNumber();

  boolean isClosed();

  void markDirty();

  boolean isMarkedDirty();
}
