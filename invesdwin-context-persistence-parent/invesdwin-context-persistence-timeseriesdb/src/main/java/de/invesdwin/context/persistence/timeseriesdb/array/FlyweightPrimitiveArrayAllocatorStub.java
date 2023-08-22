package de.invesdwin.context.persistence.timeseriesdb.array;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.stub.StubSupport;
import jakarta.inject.Named;

@Immutable
@Named
public class FlyweightPrimitiveArrayAllocatorStub extends StubSupport {

    @Override
    public void tearDownOnce(final ATest test) {
        FlyweightPrimitiveArrayAllocator.reset();
    }

}
