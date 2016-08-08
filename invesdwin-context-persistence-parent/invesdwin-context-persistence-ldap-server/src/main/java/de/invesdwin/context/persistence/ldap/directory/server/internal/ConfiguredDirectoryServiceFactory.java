package de.invesdwin.context.persistence.ldap.directory.server.internal;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.directory.server.core.factory.DefaultDirectoryServiceFactory;

import de.invesdwin.context.persistence.ldap.directory.DirectoryServerProperties;
import de.invesdwin.context.system.properties.SystemProperties;

@NotThreadSafe
public class ConfiguredDirectoryServiceFactory extends DefaultDirectoryServiceFactory {

    @Override
    public void init(final String name) throws Exception {
        new SystemProperties().setString("workingDirectory",
                new File(DirectoryServerProperties.WORKING_DIR, name).getAbsolutePath());
        super.init(name);
    }

}
