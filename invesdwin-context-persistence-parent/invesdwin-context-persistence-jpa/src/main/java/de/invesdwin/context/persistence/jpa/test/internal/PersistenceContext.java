package de.invesdwin.context.persistence.jpa.test.internal;

import javax.annotation.concurrent.Immutable;

import org.springframework.core.io.ClassPathResource;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.beans.init.locations.position.ResourcePosition;

@Immutable
public enum PersistenceContext {
    PROD_SERVER("/META-INF/ctx.persistence.jpa.prod.xml") {
        @Override
        protected void validateEnvironment() {
            if (ContextProperties.IS_TEST_ENVIRONMENT) {
                throw new IllegalArgumentException(
                        "PersistenceTests may not run on the production database. Please configure a "
                                + PersistenceContext.class.getSimpleName() + " with a TEST_* value!");
            }
        }
    },
    TEST_MEMORY("/META-INF/ctx.persistence.jpa.test.memory.xml") {
        @Override
        protected void validateEnvironment() {
            if (!ContextProperties.IS_TEST_ENVIRONMENT) {
                throw new IllegalArgumentException("PersistenceTests can not run in a production environment.");
            }
        }
    },
    TEST_SERVER("/META-INF/ctx.persistence.jpa.test.server.xml") {
        @Override
        protected void validateEnvironment() {
            TEST_MEMORY.validateEnvironment();
        }
    };

    private final String contextLocation;

    PersistenceContext(final String contextLocation) {
        this.contextLocation = contextLocation;
    }

    public String getContextLocation() {
        return contextLocation;
    }

    public PositionedResource getContextLocationResource() {
        return PositionedResource.of(new ClassPathResource(getContextLocation()), ResourcePosition.START);
    }

    protected abstract void validateEnvironment();

    public static PersistenceContext fromContextLocation(final String contextLocation) {
        for (final PersistenceContext k : PersistenceContext.values()) {
            if (contextLocation.trim().endsWith(k.getContextLocation())) {
                k.validateEnvironment();
                return k;
            }
        }
        return null;
    }

}