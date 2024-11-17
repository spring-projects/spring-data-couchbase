/*
 * Copyright 2012-2023 the original author or authors
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
package org.springframework.data.couchbase.core;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.core.mapping.event.*;
import org.springframework.data.couchbase.core.mapping.id.GeneratedValue;
import org.springframework.data.couchbase.domain.Config;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import reactor.core.publisher.Mono;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Mico Piira
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig({Config.class, CouchbaseTemplateCallbackIntegrationTests.Callbacks.class})
@DirtiesContext
class CouchbaseTemplateCallbackIntegrationTests extends JavaIntegrationTests {

    @Autowired ReactiveCouchbaseTemplate reactiveCouchbaseTemplate;
    @Autowired CouchbaseTemplate couchbaseTemplate;

    static class Callbacks {

        @Bean
        ReactiveBeforeConvertCallback<CallbacksTestEntity> reactiveBeforeConvertCallback() {
            return (entity, collection) -> Mono.just(new CallbacksTestEntity(entity.id(), entity.name() + "_beforeconvert"));
        }

        @Bean
        ReactiveAfterSaveCallback<CallbacksTestEntity> reactiveAfterSaveCallback() {
            return (entity, document, collection) -> Mono.just(new CallbacksTestEntity(entity.id(), entity.name() + "_aftersave"));
        }

        @Bean
        ReactiveBeforeSaveCallback<CallbacksTestEntity> reactiveBeforeSaveCallback() {
            return (entity, document, collection) ->
                    Mono.fromCallable(() -> document.put("name", document.get("name") + "_beforesave"))
                            .thenReturn(new CallbacksTestEntity(entity.id(), entity.name() + "_beforesave2"));
        }

        @Bean
        ReactiveAfterConvertCallback<CallbacksTestEntity> reactiveAfterConvertCallback() {
            return (entity, document, collection) -> Mono.just(new CallbacksTestEntity(entity.id(), entity.name() + "_afterconvert"));
        }

        @Bean
        BeforeConvertCallback<CouchbaseTemplateCallbackIntegrationTests.CallbacksTestEntity> BeforeConvertCallback() {
            return (entity, collection) -> new CouchbaseTemplateCallbackIntegrationTests.CallbacksTestEntity(entity.id(), entity.name() + "_beforeconvert");
        }

        @Bean
        AfterSaveCallback<CouchbaseTemplateCallbackIntegrationTests.CallbacksTestEntity> AfterSaveCallback() {
            return (entity, document, collection) -> new CouchbaseTemplateCallbackIntegrationTests.CallbacksTestEntity(entity.id(), entity.name() + "_aftersave");
        }

        @Bean
        BeforeSaveCallback<CouchbaseTemplateCallbackIntegrationTests.CallbacksTestEntity> BeforeSaveCallback() {
            return (entity, document, collection) -> {
                document.put("name", document.get("name") + "_beforesave");
                return new CouchbaseTemplateCallbackIntegrationTests.CallbacksTestEntity(entity.id(), entity.name() + "_beforesave2");
            };
        }

        @Bean
        AfterConvertCallback<CouchbaseTemplateCallbackIntegrationTests.CallbacksTestEntity> AfterConvertCallback() {
            return (entity, document, collection) -> new CouchbaseTemplateCallbackIntegrationTests.CallbacksTestEntity(entity.id(), entity.name() + "_afterconvert");
        }

    }

    public record CallbacksTestEntity(@Id @GeneratedValue String id, String name) { }

    @Test
    void testReactiveCallbacks() {
        CallbacksTestEntity entity = new CallbacksTestEntity(UUID.randomUUID().toString(), "a");
        CallbacksTestEntity saved = reactiveCouchbaseTemplate.insertById(CallbacksTestEntity.class)
                .one(entity)
                .block();
        assertEquals("a_beforesave2_aftersave", saved.name());
        CallbacksTestEntity block = reactiveCouchbaseTemplate.findById(CallbacksTestEntity.class).one(entity.id()).block();
        assertEquals("a_beforeconvert_beforesave_afterconvert", block.name());
    }

    @Test
    void testBlockingCallbacks() {
        CouchbaseTemplateCallbackIntegrationTests.CallbacksTestEntity entity = new CouchbaseTemplateCallbackIntegrationTests.CallbacksTestEntity(UUID.randomUUID().toString(), "a");
        CouchbaseTemplateCallbackIntegrationTests.CallbacksTestEntity saved = couchbaseTemplate.insertById(CouchbaseTemplateCallbackIntegrationTests.CallbacksTestEntity.class)
                .one(entity);
        assertEquals("a_beforesave2_aftersave", saved.name());
        CouchbaseTemplateCallbackIntegrationTests.CallbacksTestEntity block = couchbaseTemplate.findById(CouchbaseTemplateCallbackIntegrationTests.CallbacksTestEntity.class).one(entity.id());
        assertEquals("a_beforeconvert_beforesave_afterconvert", block.name());
    }

}
