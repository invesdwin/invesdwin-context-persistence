package de.invesdwin.context.persistence.ldap.directory;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.ldap.directory.server.DirectoryServer;

@NotThreadSafe
public final class DirectoryServerProperties {

    public static final File WORKING_DIR = new File(ContextProperties.getCacheDirectory(),
            DirectoryServer.class.getSimpleName());

    private DirectoryServerProperties() {}

}
