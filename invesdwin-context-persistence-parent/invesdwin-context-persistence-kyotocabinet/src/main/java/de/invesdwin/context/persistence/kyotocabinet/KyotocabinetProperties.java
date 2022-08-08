package de.invesdwin.context.persistence.kyotocabinet;

import java.util.List;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.collections.Collections;

@Immutable
public final class KyotocabinetProperties {

    public static final List<String> KYOTOCABINET_LIBRARY_PATHS;

    static {
        final SystemProperties systemProperties = new SystemProperties(KyotocabinetProperties.class);
        if (systemProperties.containsValue("KYOTOCABINET_LIBRARY_PATHS")) {
            KYOTOCABINET_LIBRARY_PATHS = systemProperties.getList("KYOTOCABINET_LIBRARY_PATHS");
        } else {
            KYOTOCABINET_LIBRARY_PATHS = Collections.emptyList();
        }
    }

    private KyotocabinetProperties() {
    }

}
