package de.invesdwin.context.persistence.jpa.test.internal;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.core.io.Resource;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.beans.init.locations.IContextLocation;
import de.invesdwin.context.beans.init.locations.IContextLocationValidator;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Arrays;
import jakarta.inject.Named;

/**
 * This class is relevant for rests, but it is still needed for productive usage. Thus this class resides in src instead
 * of in test.
 * 
 */
@Named
@ThreadSafe
public final class ProdPersistenceContextLocation implements IContextLocation, IContextLocationValidator {

    private static PersistenceContext activePersistenceContext;

    public static PersistenceContext getActivePersistenceContext() {
        Assertions.assertThat(activePersistenceContext).isNotNull();
        return activePersistenceContext;
    }

    @Override
    public List<PositionedResource> getContextResources() {
        if (ContextProperties.IS_TEST_ENVIRONMENT) {
            return null;
        } else {
            return Arrays.asList(PersistenceContext.PROD_SERVER.getContextLocationResource());
        }
    }

    @Override
    public void validateContextLocations(final List<PositionedResource> contextLocations) {
        boolean contextFound = false;
        for (final Resource location : contextLocations) {
            final String locationName = "/META-INF/" + location.getFilename();
            final PersistenceContext persistenceContext = PersistenceContext.fromContextLocation(locationName);
            if (persistenceContext != null) {
                if (contextFound) {
                    throw new IllegalArgumentException("More than one " + PersistenceContext.class.getSimpleName()
                            + "s found, please choose only one! Contexts: " + contextLocations);
                } else {
                    ProdPersistenceContextLocation.activePersistenceContext = persistenceContext;
                    contextFound = true;
                }
            }
        }
        if (!contextFound) {
            if (ContextProperties.IS_TEST_ENVIRONMENT) {
                //add default location, since this might be a test where ATest was not used as the base class...
                ProdPersistenceContextLocation.activePersistenceContext = PersistenceContext.TEST_MEMORY;
                contextLocations.add(activePersistenceContext.getContextLocationResource());
            } else {
                throw new IllegalArgumentException(
                        "Production persistence context not found even though this is not a test environment?!?");
            }
        }
    }

}
