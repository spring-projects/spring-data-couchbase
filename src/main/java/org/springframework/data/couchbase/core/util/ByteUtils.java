/*
 * Copyright 2012-present the original author or authors
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

package org.springframework.data.couchbase.core.util;

import java.nio.charset.StandardCharsets;

/**
 * Utility class for common byte array and String conversions using UTF-8.
 *
 * @author Emilien Bevierre
 */
public abstract class ByteUtils {

    private ByteUtils() {
        // Private constructor to prevent instantiation
    }

    /**
     * Decodes a given byte array into a String using UTF-8 charset.
     *
     * @param source the byte array to decode, can be null
     * @return the decoded string, or null if source is null
     */
    public static String getString(byte[] source) {
        return source == null ? null : new String(source, StandardCharsets.UTF_8);
    }

    /**
     * Encodes a given String into a byte array using UTF-8 charset.
     *
     * @param source the string to encode, can be null
     * @return the encoded byte array, or null if source is null
     */
    public static byte[] getBytes(String source) {
        return source == null ? null : source.getBytes(StandardCharsets.UTF_8);
    }

}
