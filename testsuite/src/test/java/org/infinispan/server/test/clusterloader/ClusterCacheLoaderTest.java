package org.infinispan.server.test.clusterloader;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.client.memcached.MemcachedClient;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * Tests ClusterLoader. When a node joins a cluster, it should NOT fetch state from the other node even though a
 * REPL mode is used (state-transfer=false). However, the node should load entries from the other node on demand (lazily).
 * ClusterLoader does not support preload nor fetchPersistentState - it acts only as a loader.
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 */
@RunWith(Arquillian.class)
public class ClusterCacheLoaderTest {

    private final String CONTAINER1 = "clusterloader-1";
    private final String CONTAINER2 = "clusterloader-2";

    @InfinispanResource(CONTAINER1)
    RemoteInfinispanServer server1;

    @InfinispanResource(CONTAINER2)
    RemoteInfinispanServer server2;

    @ArquillianResource
    ContainerController controller;

    private final String CACHE_NAME = "memcachedCache";
    private MemcachedClient mc;
    private MemcachedClient mc2;

    @Test
    public void testLazyLoadingWhenStateTransferDisabled() throws Exception {
        controller.start(CONTAINER1);
        mc = new MemcachedClient(server1.getMemcachedEndpoint().getInetAddress().getHostName(), server1.getMemcachedEndpoint()
                .getPort());
        mc.set("k1", "v1");
        mc.set("k2", "v2");
        assertEquals("v1", mc.get("k1"));
        assertEquals("v2", mc.get("k2"));
        assertEquals(2, server1.getCacheManager("clustered").getCache(CACHE_NAME).getNumberOfEntries());
        controller.start(CONTAINER2);
        mc2 = new MemcachedClient(server2.getMemcachedEndpoint().getInetAddress().getHostName(), server2.getMemcachedEndpoint()
                .getPort());
        assertEquals(2, server2.getCacheManager("clustered").getClusterSize());
        //state-transfer = false -> no entries in the newly joined node
        assertEquals(0, server2.getCacheManager("clustered").getCache(CACHE_NAME).getNumberOfEntries());
        assertEquals("v1", mc2.get("k1")); //lazily load the entries
        assertEquals("v2", mc2.get("k2"));
        assertEquals(2, server2.getCacheManager("clustered").getCache(CACHE_NAME).getNumberOfEntries());
        mc2.set("k3", "v3");
        assertEquals(3, server2.getCacheManager("clustered").getCache(CACHE_NAME).getNumberOfEntries());
        assertEquals(3, server1.getCacheManager("clustered").getCache(CACHE_NAME).getNumberOfEntries());
        controller.kill(CONTAINER2);
        controller.kill(CONTAINER1);
    }
}
