package org.springframework.data.couchbase.core

import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.annotation.Id
import org.springframework.data.couchbase.ContainerResourceRunner
import org.springframework.data.couchbase.IntegrationTestApplicationConfig
import org.springframework.test.context.ContextConfiguration
import kotlin.test.assertEquals

@RunWith(ContainerResourceRunner::class)
@ContextConfiguration(classes = [IntegrationTestApplicationConfig::class])
class CouchbaseOperationsExtensionsIntegrationTest {

    @Autowired
    lateinit var template: CouchbaseTemplate

    @Test
    fun `findById should call the reified extension`()  {
        val entityId = "CouchbaseOperationsExtensionsTestFindById"
        val entity = Entity(entityId)
        template.save(entity)
        val stored = template.findById<Entity>(entityId)
        assertEquals(entity.id, stored.id)
    }

    class Entity constructor(id: String) {
        @Id
        var id = id
    }
}
