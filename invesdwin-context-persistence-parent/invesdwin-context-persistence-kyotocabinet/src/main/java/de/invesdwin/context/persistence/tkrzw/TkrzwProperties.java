package de.invesdwin.context.persistence.tkrzw;

import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.properties.SystemProperties;

@Immutable
public final class TkrzwProperties {

    public static final List<String> TKRZW_LIBRARY_PATHS;

    static {
        final SystemProperties systemProperties = new SystemProperties(TkrzwProperties.class);
        if (systemProperties.containsValue("TKRZW_LIBRARY_PATHS")) {
            TKRZW_LIBRARY_PATHS = systemProperties.getList("TKRZW_LIBRARY_PATHS");
        } else {
            TKRZW_LIBRARY_PATHS = Collections.emptyList();
        }
    }

    private TkrzwProperties() {
    }

}
