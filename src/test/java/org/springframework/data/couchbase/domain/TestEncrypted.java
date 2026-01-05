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

package org.springframework.data.couchbase.domain;

import java.io.Serializable;
import java.util.Objects;

import org.springframework.data.couchbase.core.mapping.Document;

import com.couchbase.client.java.encryption.annotation.Encrypted;

/**
 * UserEncrypted entity for tests
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */

@Document
public class TestEncrypted implements Serializable {

	public String id;
	@Encrypted
	public byte[] encString={1,2,3,4};

	public TestEncrypted() {
	}

	public TestEncrypted(final String id) {
		this();
		this.id = id;
	}

	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}


	public String toString(){
		StringBuffer sb=new StringBuffer();
		sb.append("encString: "+encToString());
		return sb.toString();
	}

	@Override
	public int hashCode() {
		return Objects.hash(id);
	}

	public void initSimpleTypes(){

	}

	@Override  public boolean equals(Object o){
		if(o == null || o.getClass() != getClass()){
			return false;
		}
		TestEncrypted other = (TestEncrypted) o;
		//return this.encString == other.encString;
		if(other.encString == null && this.encString != null)
			return false;
		return other.encString.equals(this.encString);
	}

	public String encToString(){
		StringBuffer sb = new StringBuffer();
		for(byte c:encString){
			if(!sb.isEmpty())
				sb.append(",");
			sb.append(c);
		}
		return sb.toString();
	}
}
