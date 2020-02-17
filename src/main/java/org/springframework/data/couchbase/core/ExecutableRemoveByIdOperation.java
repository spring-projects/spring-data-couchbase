package org.springframework.data.couchbase.core;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface ExecutableRemoveByIdOperation {

  ExecutableRemoveById removeById();

  interface TerminatingRemoveById {

    RemoveResult one(String id);

    List<RemoveResult> all(Collection<String> ids);

  }

  interface RemoveByIdWithCollection extends TerminatingRemoveById {

    TerminatingRemoveById inCollection(String collection);
  }

  interface RemoveByIdWithDurability extends RemoveByIdWithCollection {

    RemoveByIdWithCollection withDurability(DurabilityLevel durabilityLevel);

    RemoveByIdWithCollection withDurability(PersistTo persistTo, ReplicateTo replicateTo);

  }

  interface ExecutableRemoveById extends RemoveByIdWithDurability {}

}
