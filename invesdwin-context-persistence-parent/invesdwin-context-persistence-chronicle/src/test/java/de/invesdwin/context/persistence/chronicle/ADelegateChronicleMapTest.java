package de.invesdwin.context.persistence.chronicle;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.IntegerSerde;
import de.invesdwin.util.marshallers.serde.basic.StringUtf8Serde;
import net.openhft.chronicle.map.ChronicleMapBuilder;

// CHECKSTYLE:OFF
@NotThreadSafe
public class ADelegateChronicleMapTest extends ATest {
    //CHECKSTYLE:ON

    @Test
    public void testItWorks() {
        Throwables.setDebugStackTraceEnabled(true);
        final ADelegateChronicleMap<String, Integer> map = new ADelegateChronicleMap<String, Integer>("testItWorks") {
            @Override
            protected File getBaseDirectory() {
                return ContextProperties.TEMP_DIRECTORY;
            }

            @Override
            protected ChronicleMapBuilder configureMap(final ChronicleMapBuilder builder) {
                return builder.averageKeySize(1).averageValueSize(10);
            }

            @Override
            protected ISerde<String> newKeySerde() {
                return StringUtf8Serde.GET;
            }

            @Override
            protected ISerde<Integer> newValueSerde() {
                return IntegerSerde.GET;
            }

            //            @Override
            //            protected ICompressionFactory getCompressionFactory() {
            //                return DisabledCompressionFactory.INSTANCE;
            //            }
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
