package de.invesdwin.context.persistence.ldap.directory.test.internal;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Named;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;

import de.invesdwin.context.ContextDirectoriesStub;
import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.persistence.ldap.directory.DirectoryServerContextLocation;
import de.invesdwin.context.persistence.ldap.directory.DirectoryServerProperties;
import de.invesdwin.context.persistence.ldap.directory.server.DirectoryServer;
import de.invesdwin.context.persistence.ldap.directory.test.DirectoryServerTest;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.TestContext;
import de.invesdwin.context.test.stub.StubSupport;
import de.invesdwin.util.lang.Reflections;
import de.invesdwin.util.shutdown.IShutdownHook;
import de.invesdwin.util.shutdown.ShutdownHookManager;

@Named
@NotThreadSafe
public class DirectoryServerTestStub extends StubSupport {

    private static volatile DirectoryServer lastServer;

    static {
        ShutdownHookManager.register(new IShutdownHook() {
            @Override
            public void shutdown() throws Exception {
                maybeStopLastServer();
            }
        });
        ContextDirectoriesStub.addProtectedDirectory(DirectoryServerProperties.WORKING_DIR);
    }

    @Override
    public void setUpContextLocations(final ATest test, final List<PositionedResource> locations) throws Exception {
        //if for some reason the tearDownOnce was not executed on the last test (maybe maven killed it?), then try to stop here aswell
        maybeStopLastServer();
        final DirectoryServerTest annotation = Reflections.getAnnotation(test, DirectoryServerTest.class);
        if (annotation != null) {
            if (annotation.value()) {
                locations.add(DirectoryServerContextLocation.CONTEXT_LOCATION);
            } else {
                locations.remove(DirectoryServerContextLocation.CONTEXT_LOCATION);
            }
        }
    }

    @Override
    public void setUpContext(final ATest test, final TestContext ctx) throws Exception {
        //clean up for next test
        try {
            FileUtils.deleteDirectory(DirectoryServerProperties.WORKING_DIR);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setUpOnce(final ATest test, final TestContext ctx) throws Exception {
        try {
            lastServer = MergedContext.getInstance().getBean(DirectoryServer.class);
        } catch (final NoSuchBeanDefinitionException e) { //SUPPRESS CHECKSTYLE empty block
            //ignore
        }
    }

    @Override
    public void tearDownOnce(final ATest test) throws Exception {
        maybeStopLastServer();
    }

    private static void maybeStopLastServer() throws Exception {
        if (lastServer != null) {
            IntegrationProperties.setWebserverTest(false);
            lastServer.stop();
            lastServer = null;
        }
    }

}
