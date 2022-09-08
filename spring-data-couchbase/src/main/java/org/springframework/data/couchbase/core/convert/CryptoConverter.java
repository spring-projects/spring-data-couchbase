/*
 * Copyright 2022 the original author or authors
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
package org.springframework.data.couchbase.core.convert;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.springframework.core.convert.ConversionService;
import org.springframework.data.convert.PropertyValueConverter;
import org.springframework.data.convert.ValueConversionContext;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.util.Assert;

import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.java.json.JsonObject;

/**
 * Encrypt/Decrypted properties annotated with
 * 
 * @author Michael Reiche
 */
public class CryptoConverter implements
		PropertyValueConverter<Object, CouchbaseDocument, ValueConversionContext<? extends PersistentProperty<?>>> {

	CryptoManager cryptoManager;

	public CryptoConverter(CryptoManager cryptoManager) {
		this.cryptoManager = cryptoManager;
	}

	@Override
	public Object read(CouchbaseDocument value, ValueConversionContext<? extends PersistentProperty<?>> context) {
		byte[] decrypted = cryptoManager().decrypt(value.export());
		if (decrypted == null) {
			return null;
		}
		// it's decrypted into a String. Now figure out how to convert to the property type.
		CouchbaseConversionContext ctx = (CouchbaseConversionContext) context;
		CouchbasePersistentProperty property = ctx.getProperty();
		org.springframework.data.convert.CustomConversions conversions = ctx.getConverter().getConversions();
		ConversionService svc = ctx.getConverter().conversionService;

		boolean wasString = false;
		List<Exception> exceptionList = new LinkedList<>();
		String decryptedString = new String(decrypted);
		if (conversions.isSimpleType(property.getType())
				|| conversions.hasCustomReadTarget(String.class, context.getProperty().getType())) {
			if (decryptedString.startsWith("\"") && decryptedString.endsWith("\"")) {
				decryptedString = decryptedString.substring(1, decryptedString.length() - 1);
				decryptedString = decryptedString.replaceAll("\\\"", "\"");
				wasString = true;
			}
		}

		if (conversions.isSimpleType(property.getType())) {
			if (wasString && conversions.hasCustomReadTarget(String.class, context.getProperty().getType())) {
				try {
					return svc.convert(decryptedString, context.getProperty().getType());
				} catch (Exception e) {
					exceptionList.add(e);
				}
			}
			if (conversions.hasCustomReadTarget(Long.class, context.getProperty().getType())) {
				try {
					return svc.convert(Long.valueOf(decryptedString), property.getType());
				} catch (Exception e) {
					exceptionList.add(e);
				}
			}
			if (conversions.hasCustomReadTarget(Double.class, context.getProperty().getType())) {
				try {
					return svc.convert(Double.valueOf(decryptedString), property.getType());
				} catch (Exception e) {
					exceptionList.add(e);
				}
			}
			if (conversions.hasCustomReadTarget(Boolean.class, context.getProperty().getType())) {
				try {
					Object booleanResult = svc.convert(Boolean.valueOf(decryptedString), property.getType());
					if (booleanResult == null) {
						throw new Exception("value " + decryptedString + " would not convert to boolean");
					}
				} catch (Exception e) {
					exceptionList.add(e);
				}
			}
			// let's try to find a constructor...
			try {
				Constructor<?> constructor = context.getProperty().getType().getConstructor(String.class);
				return constructor.newInstance(decryptedString);
			} catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
				exceptionList.add(new Exception("tried to instantiate from constructor taking string arg but got " + e));
			}
			// last chance...
			try {
				return ctx.getConverter().getPotentiallyConvertedSimpleRead(decryptedString, property);
			} catch (Exception e) {
				exceptionList.add(e);
				RuntimeException ee = new RuntimeException(
						"failed to convert " + decryptedString + " due to the following suppressed reasons(s): ");
				exceptionList.stream().forEach(ee::addSuppressed);
				throw ee;
			}

		} else {
			CouchbaseDocument decryptedDoc = new CouchbaseDocument().setContent(JsonObject.fromJson(decrypted));
			CouchbasePersistentEntity<?> entity = ctx.getConverter().getMappingContext()
					.getRequiredPersistentEntity(property.getType());
			return ctx.getConverter().read(entity, decryptedDoc, null);
		}
	}

	@Override
	public CouchbaseDocument write(Object value, ValueConversionContext<? extends PersistentProperty<?>> context) {
		byte[] plainText;
		CouchbaseConversionContext ctx = (CouchbaseConversionContext) context;
		CouchbasePersistentProperty property = ctx.getProperty();
		org.springframework.data.convert.CustomConversions conversions = ctx.getConverter().getConversions();

		Class<?> sourceType = context.getProperty().getType();
		Class<?> targetType = conversions.getCustomWriteTarget(context.getProperty().getType()).orElse(null);

		value = ctx.getConverter().getPotentiallyConvertedSimpleWrite(property, ctx.getAccessor(), false);
		if (conversions.isSimpleType(sourceType)) {
			String plainString;
			plainString = (String) value;
			if (sourceType == String.class || targetType == String.class) {
				plainString = "\"" + plainString.replaceAll("\"", "\\\"") + "\"";
			}
			plainText = plainString.getBytes(StandardCharsets.UTF_8);
		} else {
			plainText = JsonObject.fromJson(context.read(value).toString().getBytes(StandardCharsets.UTF_8)).toBytes();
		}
		Map<String, Object> encrypted = cryptoManager().encrypt(plainText, CryptoManager.DEFAULT_ENCRYPTER_ALIAS);
		CouchbaseDocument encryptedDoc = new CouchbaseDocument();
		for (Map.Entry<String, Object> entry : encrypted.entrySet()) {
			encryptedDoc.put(entry.getKey(), entry.getValue());
		}
		return encryptedDoc;
	}

	CryptoManager cryptoManager() {
		Assert.notNull(cryptoManager, "cryptoManager is null");
		return cryptoManager;
	}

}
