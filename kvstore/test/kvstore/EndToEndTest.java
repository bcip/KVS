package kvstore;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class EndToEndTest extends EndToEndTemplate {

    @Test
    public void testPutGet() throws KVException {
        client.put("foo", "bar");
        assertEquals(client.get("foo"), "bar");
    }

}
