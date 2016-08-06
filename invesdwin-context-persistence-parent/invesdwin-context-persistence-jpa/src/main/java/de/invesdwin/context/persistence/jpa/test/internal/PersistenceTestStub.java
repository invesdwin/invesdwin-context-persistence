package de.invesdwin.context.persistence.jpa.test.internal;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Named;

import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.persistence.jpa.test.PersistenceTest;
import de.invesdwin.context.persistence.jpa.test.PersistenceTestContext;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.stub.StubSupport;
import de.invesdwin.util.lang.Reflections;

@Named
@NotThreadSafe
public class PersistenceTestStub extends StubSupport {

    @Override
    public void setUpContextLocations(final ATest test, final List<PositionedResource> locations) throws Exception {
        final PersistenceTest annotation = Reflections.getAnnotation(test, PersistenceTest.class);
        if (annotation != null && annotation.value() != null) {
            locations.add(annotation.value().getPersistenceContext().getContextLocationResource());
        } else {
            locations.add(PersistenceTestContext.MEMORY.getPersistenceContext().getContextLocationResource());
        }
    }

}
