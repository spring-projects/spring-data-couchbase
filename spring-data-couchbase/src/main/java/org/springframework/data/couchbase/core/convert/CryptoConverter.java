package org.springframework.data.couchbase.core.convert;

import java.nio.charset.StandardCharsets;
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

		String decryptedString = new String(decrypted);
		if (conversions.isSimpleType(property.getType())
				|| conversions.hasCustomReadTarget(String.class, context.getProperty().getType())) {
			if (decryptedString.startsWith("\"") && decryptedString.endsWith("\"")) {
				decryptedString = decryptedString.substring(1, decryptedString.length() - 1);
			}
		}

		if (conversions.isSimpleType(property.getType())) {
			if (conversions.hasCustomReadTarget(String.class, context.getProperty().getType())) {
				try {
					return svc.convert(decryptedString, context.getProperty().getType());
				} catch (Exception e) {
					System.err.println(e);
				}
			}
			if (conversions.hasCustomReadTarget(Long.class, context.getProperty().getType())) {
				try {
					return svc.convert(new Long(decryptedString), property.getType());
				} catch (Exception e) {
					System.err.println(e);
				}
			}
			if (conversions.hasCustomReadTarget(Double.class, context.getProperty().getType())) {
				try {
					return svc.convert(new Double(decryptedString), property.getType());
				} catch(Exception e) {
					System.err.println(e);
				}
				throw new RuntimeException("ran out of conversions to try");
			} else {
				return ctx.getConverter().getPotentiallyConvertedSimpleRead(decryptedString, property);
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

		value = ctx.getConverter().getPotentiallyConvertedSimpleWrite2(property, ctx.getAccessor());

		if (conversions.isSimpleType(sourceType)) {
			String plainString;
			if (sourceType == String.class || targetType == String.class) {
				plainString = (String) value;
				plainString = "\"" + plainString + "\"";
				plainText = plainString.getBytes(StandardCharsets.UTF_8);
			} else {
				plainString = value.toString();
				plainText = plainString.getBytes(StandardCharsets.UTF_8);
			}
		} else {
			plainText = JsonObject.fromJson(context.read(value).toString().getBytes(StandardCharsets.UTF_8)).toBytes();
		}
		Map<String, Object> encrypted = cryptoManager().encrypt(plainText, "__DEFAULT__");
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
