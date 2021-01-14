/*
 * Copyright 2020 the original author or authors
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

package org.springframework.data.couchbase.util;

import java.io.InputStream;
import java.time.Duration;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.awaitility.Awaitility.with;

/**
 * Provides a bunch of utility APIs that help with testing.
 */
public class Util {

  /**
   * Waits and sleeps for a little bit of time until the given condition is met.
   *
   * <p>Sleeps 1ms between "false" invocations. It will wait at most one minute to prevent hanging forever in case
   * the condition never becomes true.</p>
   *
   * @param supplier return true once it should stop waiting.
   */
  public static void waitUntilCondition(final BooleanSupplier supplier) {
    waitUntilCondition(supplier, Duration.ofMinutes(1));
  }

  public static void waitUntilCondition(final BooleanSupplier supplier, Duration atMost) {
    with().pollInterval(Duration.ofMillis(1)).await().atMost(atMost).until(supplier::getAsBoolean);
  }

  public static void waitUntilCondition(final BooleanSupplier supplier, Duration atMost, Duration delay) {
    with().pollInterval(delay).await().atMost(atMost).until(supplier::getAsBoolean);
  }

  public static void waitUntilThrows(final Class<? extends Exception> clazz, final Supplier<Object> supplier) {
    with()
        .pollInterval(Duration.ofMillis(1))
        .await()
        .atMost(Duration.ofMinutes(1))
        .until(() -> {
          try {
            supplier.get();
          } catch (final Exception ex) {
            return ex.getClass().isAssignableFrom(clazz);
          }
          return false;
        });
  }

  /**
   * Returns true if a thread with the given name is currently running.
   *
   * @param name the name of the thread.
   * @return true if running, false otherwise.
   */
  public static boolean threadRunning(final String name) {
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.getName().equalsIgnoreCase(name)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Reads a file from the resources folder (in the same path as the requesting test class).
   *
   * <p>The class will be automatically loaded relative to the namespace and converted
   * to a string.</p>
   *
   * @param filename the filename of the resource.
   * @param clazz    the reference class.
   * @return the loaded string.
   */
  public static String readResource(final String filename, final Class<?> clazz) {
    String path = "/" + clazz.getPackage().getName().replace(".", "/") + "/" + filename;
    InputStream stream = clazz.getResourceAsStream(path);
    java.util.Scanner s = new java.util.Scanner(stream, UTF_8.name()).useDelimiter("\\A");
    return s.hasNext() ? s.next() : "";
  }

}
