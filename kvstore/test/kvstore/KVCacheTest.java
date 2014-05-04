package kvstore;

import static org.junit.Assert.*;

import org.junit.*;

public class KVCacheTest {

    /**
     * Verify the cache can put and get a KV pair successfully.
     */
    @Test
    public void singlePutAndGet() {
        KVCache cache = new KVCache(1, 4);
        cache.put("hello", "world");
        assertEquals("world", cache.get("hello"));
    }

}
