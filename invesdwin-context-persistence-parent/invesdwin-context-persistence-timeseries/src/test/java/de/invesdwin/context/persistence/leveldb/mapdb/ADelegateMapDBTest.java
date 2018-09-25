package de.invesdwin.context.persistence.leveldb.mapdb;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

// CHECKSTYLE:OFF
@NotThreadSafe
public class ADelegateMapDBTest extends ATest {
    //CHECKSTYLE:ON

    @Test
    public void testItWorks() {
        final ADelegateMapDB<String, Integer> map = new ADelegateMapDB<String, Integer>("testItWorks") {
            @Override
            protected File getBaseDirectory() {
                return ContextProperties.TEMP_DIRECTORY;
            }
        };
        Assertions.assertThat(map).isEmpty();
        Assertions.assertThat(map.put("1", 1)).isNull();
        Assertions.assertThat(map.get("1")).isEqualTo(1);
        Assertions.assertThat(map).isNotEmpty();
        map.close();
        Assertions.assertThat(map.isEmpty()).isFalse();
        map.clear();
        Assertions.assertThat(map.isEmpty()).isTrue();
        map.close();
    }

}
