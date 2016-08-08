package de.invesdwin.context.persistence.ldap.directory;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.stub.StubSupport;

@Named
@Immutable
public class DirectoryServerContextLocationStub extends StubSupport {

    @Override
    public void tearDownOnce(final ATest test) {
        DirectoryServerContextLocation.deactivate();
    }

}
