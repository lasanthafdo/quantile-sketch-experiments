import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import redis.clients.jedis.Jedis;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class NewSetupGeneratorTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    @Mock
    Jedis jedis;

    @Test
    public void testNewSetup() {
        NewSetupGenerator newSetupGenerator = new NewSetupGenerator(jedis);

        newSetupGenerator.setupWorkload();
        verify(jedis, times(1)).flushAll();
        verify(jedis, times(100)).sadd(any(String.class), any(String.class));
    }

}
