/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.util;

import com.couchbase.client.ClusterManager;
import com.couchbase.client.clustermanager.BucketType;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.compat.SpyObject;

import java.net.SocketAddress;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A helper class for test cases that retries bucket creation, deletion, and
 * warmup since these processes can take a long time.
 *
 * @author Michael Nitschinger
 */
public class BucketManager extends SpyObject {

  private static final long TIMEOUT = 15000;
  private static final long SLEEP_TIME = 1000;

  private final ClusterManager manager;

  public BucketManager(String host, String admin, String pass) {
    List<URI> uris = new LinkedList<URI>();
    uris.add(URI.create(host));
    manager = new ClusterManager(uris, admin, pass);
  }

  /**
   * A class for defining a simple callback. Used to define your own poll
   * function.
   */
  public static class FunctionCallback {
    public void callback() throws Exception {
      throw new UnsupportedOperationException("Must override this function");
    }

    public String success(long elapsedTime) {
      throw new UnsupportedOperationException("Must override this function");
    }
  }

  public void deleteAllBuckets() throws Exception {
    FunctionCallback callback = new FunctionCallback() {
      @Override
      public void callback() throws Exception {
        List<String> buckets = manager.listBuckets();
        for (int i = 0; i < buckets.size(); i++) {
          manager.deleteBucket(buckets.get(i));
        }
      }

      @Override
      public String success(long elapsedTime) {
        return "Bucket deletion took " + elapsedTime + "ms";
      }
    };
    poll(callback);
  }

  public void createDefaultBucket(final BucketType type, final int quota,
                                  final int replicas, final boolean flush) throws Exception {
    FunctionCallback callback = new FunctionCallback() {
      @Override
      public void callback() throws Exception {
        manager.createDefaultBucket(type, quota, replicas, flush);
      }

      @Override
      public String success(long elapsedTime) {
        return "Bucket creation took " + elapsedTime + "ms";
      }
    };
    poll(callback);
  }

  public void createSaslBucket(final String name, final BucketType type,
                               final int quota, final int replicas, final boolean flush) throws Exception {
    FunctionCallback callback = new FunctionCallback() {
      @Override
      public void callback() throws Exception {
        manager.createNamedBucket(type, name, quota, replicas, name, flush);
      }

      @Override
      public String success(long elapsedTime) {
        return "Bucket creation took " + elapsedTime + "ms";
      }
    };
    poll(callback);
  }

  public void poll(FunctionCallback cb) throws Exception {
    long st = System.currentTimeMillis();

    while (true) {
      try {
        cb.callback();
        getLogger().info(cb.success(System.currentTimeMillis() - st));
        return;
      } catch (RuntimeException e) {
        if ((System.currentTimeMillis() - st) > TIMEOUT) {
          throw e;
        }
        Thread.sleep(SLEEP_TIME);
      }
    }
  }

  public void waitForWarmup(MemcachedClient client) throws Exception {
    boolean warmup = true;
    while (warmup) {
      warmup = false;
      Map<SocketAddress, Map<String, String>> stats = client.getStats();
      for (Entry<SocketAddress, Map<String, String>> server: stats.entrySet()) {
        Map<String, String> serverStats = server.getValue();
        if (!serverStats.containsKey("ep_degraded_mode")) {
          warmup = true;
          Thread.sleep(1000);
          break;
        }
        if (!serverStats.get("ep_degraded_mode").equals("0")) {
          warmup = true;
          Thread.sleep(1000);
          break;
        }
      }
    }
  }
}