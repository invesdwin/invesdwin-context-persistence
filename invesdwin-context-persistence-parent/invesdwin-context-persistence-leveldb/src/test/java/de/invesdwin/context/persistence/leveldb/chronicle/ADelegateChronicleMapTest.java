package de.invesdwin.context.persistence.leveldb.chronicle;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

// CHECKSTYLE:OFF
@NotThreadSafe
public class ADelegateChronicleMapTest extends ATest {
    //CHECKSTYLE:ON

    @Test
    public void testItWorks() {
        final ADelegateChronicleMap<String, Integer> map = new ADelegateChronicleMap<String, Integer>("testItWorks") {
            @Override
            protected File getBaseDirectory() {
                return ContextProperties.TEMP_DIRECTORY;
            }

            @Override
            protected String getKeyAverageSample() {
                return "100";
            }

            @Override
            protected Integer getValueAverageSample() {
                return 100;
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
