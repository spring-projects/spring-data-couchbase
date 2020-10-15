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

package org.springframework.data.couchbase.domain;

import java.lang.reflect.Field;

/**
 * Comparable entity base class for tests
 *
 * @author Michael Reiche
 */
public class ComparableEntity {

	/**
	 * equals() method that recursively calls equals on on fields
	 * 
	 * @param that
	 * @return
	 * @throws RuntimeException
	 */
	@Override
	public boolean equals(Object that) throws RuntimeException {
		if (this == that) {
			return true;
		}
		if (that == null
				|| !(this.getClass().isAssignableFrom(that.getClass()) || that.getClass().isAssignableFrom(this.getClass()))) {
			return false;
		}
		// check that all the fields in this have an equal field in that
		for (Field f : this.getClass().getFields()) {
			if (!same(f, this, that)) {
				return false;
			}
		}
		// check that all the fields in that have an equal field in this
		for (Field f : that.getClass().getFields()) {
			if (!same(f, that, this)) {
				return false;
			}
		}
		// check that all the declared fields in this have an equal field in that
		for (Field f : this.getClass().getDeclaredFields()) {
			if (!same(f, this, that)) {
				return false;
			}
		}
		// check that all the declared fields in that have an equal field in this
		for (Field f : that.getClass().getDeclaredFields()) {
			if (!same(f, that, this)) {
				return false;
			}
		}
		return true;
	}

	private static boolean same(Field f, Object a, Object b) {
		Object thisField = null;
		Object thatField = null;

		try {
			thisField = f.get(a);
			thatField = f.get(b);
		} catch (IllegalAccessException e) {
			// assume that the important fields are in toString()
			thisField = a.toString();
			thatField = b.toString();
		}
		if (thisField == null && thatField == null) {
			return true;
		}
		if (thisField == null && thatField != null) {
			return false;
		}
		if (!thisField.equals(thatField)) {
			return false;
		}
		return true;
	}
}
