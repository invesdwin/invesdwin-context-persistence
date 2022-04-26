package de.invesdwin.context.persistence.tokyocabinet;

import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.properties.SystemProperties;

@Immutable
public final class TokyocabinetProperties {

    public static final List<String> TOKYOCABINET_LIBRARY_PATHS;

    static {
        final SystemProperties systemProperties = new SystemProperties(TokyocabinetProperties.class);
        if (systemProperties.containsValue("TOKYOCABINET_LIBRARY_PATHS")) {
            TOKYOCABINET_LIBRARY_PATHS = systemProperties.getList("TOKYOCABINET_LIBRARY_PATHS");
        } else {
            TOKYOCABINET_LIBRARY_PATHS = Collections.emptyList();
        }
    }

    private TokyocabinetProperties() {
    }

}
