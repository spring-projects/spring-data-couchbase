/*
 * Copyright 2011-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository.support;

import static com.querydsl.apt.APTOptions.QUERYDSL_LOG_INFO;

import java.util.Collections;
import java.util.Set;

import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;

import org.springframework.data.couchbase.core.mapping.Document;

import com.querydsl.apt.AbstractQuerydslProcessor;
import com.querydsl.apt.Configuration;
import com.querydsl.apt.DefaultConfiguration;
import com.querydsl.core.annotations.QueryEmbeddable;
import com.querydsl.core.annotations.QueryEmbedded;
import com.querydsl.core.annotations.QueryEntities;
import com.querydsl.core.annotations.QuerySupertype;
import com.querydsl.core.annotations.QueryTransient;
import org.springframework.lang.Nullable;

/**
 * Annotation processor to create Querydsl query types for QueryDsl annotated classes.
 *
 * @author Michael Reiche
 */
@SupportedAnnotationTypes({ "com.querydsl.core.annotations.*", "org.springframework.data.couchbase.core.mapping.*" })
@SupportedSourceVersion(SourceVersion.RELEASE_6)
public class CouchbaseAnnotationProcessor extends AbstractQuerydslProcessor {

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.apt.AbstractQuerydslProcessor#createConfiguration(javax.annotation.processing.RoundEnvironment)
	 */
	@Override
	protected Configuration createConfiguration(@Nullable RoundEnvironment roundEnv) {

		processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "Running override createConfiguration() " + getClass().getSimpleName());

		DefaultConfiguration configuration = new DefaultConfiguration(processingEnv, roundEnv, Collections.emptySet(),
				QueryEntities.class, Document.class, QuerySupertype.class, QueryEmbeddable.class, QueryEmbedded.class,
				QueryTransient.class);
		configuration.setUnknownAsEmbedded(true);

		return configuration;
	}

  @Override
	public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
		setLogInfo();
		logInfo("Running override process() " + getClass().getSimpleName() +" isOver: "+roundEnv.processingOver() +" annotations: "+annotations.size());

		if (roundEnv.processingOver() || annotations.size() == 0) {
			return ALLOW_OTHER_PROCESSORS_TO_CLAIM_ANNOTATIONS;
		}

		if (roundEnv.getRootElements() == null || roundEnv.getRootElements().isEmpty()) {
			logInfo("No sources to process");
			return ALLOW_OTHER_PROCESSORS_TO_CLAIM_ANNOTATIONS;
		}

    Configuration conf = createConfiguration(roundEnv);
		try {
			conf.getTypeMappings();
		} catch (NoClassDefFoundError cnfe ){
			logWarn( cnfe +" add a dependency on javax.inject:javax.inject to create querydsl classes");
			return ALLOW_OTHER_PROCESSORS_TO_CLAIM_ANNOTATIONS;
		}
		return super.process(annotations, roundEnv);

	}

	private boolean shouldLogInfo;

	private void setLogInfo() {
		boolean hasProperty = processingEnv.getOptions().containsKey(QUERYDSL_LOG_INFO);
		if (hasProperty) {
			String val = processingEnv.getOptions().get(QUERYDSL_LOG_INFO);
			shouldLogInfo = Boolean.parseBoolean(val);
		}
	}

	private void logInfo(String message) {
		if (shouldLogInfo) {
			System.out.println("[NOTE] "+message); // maven compiler swallows messages to messager
			processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, message);
		}
	}

	private void logWarn(String message) {
			System.err.println("[WARNING] "+message); // maven compiler swallows messages to messager
			processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING, message);
	}
}

