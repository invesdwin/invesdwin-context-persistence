package de.invesdwin.context.persistence.mapdb;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.persistentmap.APersistentMap;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.error.Throwables;

// CHECKSTYLE:OFF
@NotThreadSafe
public class PersistentMapDBFactoryTest extends ATest {
    //CHECKSTYLE:ON

    @Test
    public void testItWorks() {
        Throwables.setDebugStackTraceEnabled(true);
        final APersistentMap<String, Integer> map = new APersistentMap<String, Integer>("testItWorks") {
            @Override
            public File getBaseDirectory() {
                return ContextProperties.TEMP_DIRECTORY;
            }

            @Override
            protected IPersistentMapFactory<String, Integer> newFactory() {
                return new PersistentMapDBFactory<>();
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
