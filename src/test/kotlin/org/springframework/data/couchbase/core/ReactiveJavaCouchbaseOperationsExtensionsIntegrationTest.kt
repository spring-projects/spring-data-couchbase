package org.springframework.data.couchbase.core

import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.annotation.Id
import org.springframework.data.couchbase.ContainerResourceRunner
import org.springframework.data.couchbase.ReactiveIntegrationTestApplicationConfig
import org.springframework.test.context.ContextConfiguration
import kotlin.test.assertEquals


@RunWith(ContainerResourceRunner::class)
@ContextConfiguration(classes = [ReactiveIntegrationTestApplicationConfig::class])
class ReactiveJavaCouchbaseOperationsExtensionsIntegrationTest {

    @Autowired
    lateinit var template: ReactiveJavaCouchbaseTemplate

    @Test
    fun `findById should call the reified extension`()  {
        val entityId = "ReactiveCouchbaseOperationsExtensionsTestFindById"
        val entity = Entity(entityId)
        template.save(entity).block();
        val stored = template.findById<Entity>(entityId).block();
        assertEquals(entity.id, stored.id)
    }

    class Entity {
        @Id
        var id : String = ""

        constructor(id: String) {
            this.id = id
        }

        constructor() {
            this.id = ""
        }
    }
}

