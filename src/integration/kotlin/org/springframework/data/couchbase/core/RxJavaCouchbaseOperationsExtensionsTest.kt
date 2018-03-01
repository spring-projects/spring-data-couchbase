package org.springframework.data.couchbase.core

import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.annotation.Id
import org.springframework.data.couchbase.ContainerResourceRunner
import org.springframework.data.couchbase.ReactiveIntegrationTestApplicationConfig
import org.springframework.data.couchbase.core.query.N1qlSecondaryIndexed
import org.springframework.test.context.ContextConfiguration
import kotlin.test.assertEquals


@RunWith(ContainerResourceRunner::class)
@ContextConfiguration(classes = [ReactiveIntegrationTestApplicationConfig::class])
class RxJavaCouchbaseOperationsExtensionsTest {

    @Autowired
    lateinit var template: RxJavaCouchbaseTemplate

    @Test
    fun `findById should call the reified extension`()  {
        val entityId = "RxJavaCouchbaseOperationsExtensionsTestFindById"
        val entity = Entity(entityId)
        template.save(entity).toBlocking().single()
        val stored = template.findById<Entity>(entityId).toBlocking().single()
        assertEquals(entity.id, stored.id)
    }

    class Entity constructor(id: String) {
        @Id
        var id = id
    }
}

