package de.invesdwin.context.persistence.jpa.api.util;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.string.Strings;
import jakarta.persistence.metamodel.Attribute;
import jakarta.persistence.metamodel.Attribute.PersistentAttributeType;
import jakarta.persistence.metamodel.ManagedType;

@NotThreadSafe
public final class Attributes {

    public static final String ID_SUFFIX = "_id";

    private Attributes() {}

    public static String extractNativeSqlColumnName(final Attribute<?, ?> attr) {
        switch (attr.getPersistentAttributeType()) {
        case BASIC:
        case EMBEDDED:
            return attr.getName();
        case ELEMENT_COLLECTION:
        case ONE_TO_ONE:
        case MANY_TO_ONE:
            return attr.getName() + ID_SUFFIX;
        case MANY_TO_MANY:
        case ONE_TO_MANY:
            throw new IllegalArgumentException(PersistentAttributeType.class.getSimpleName() + " ["
                    + attr.getPersistentAttributeType() + "] is not supported!");
        default:
            throw UnknownArgumentException.newInstance(PersistentAttributeType.class,
                    attr.getPersistentAttributeType());
        }
    }

    public static Attribute<?, ?> findAttribute(final ManagedType<?> managedType, final String columnName) {
        try {
            return managedType.getAttribute(Strings.removeEnd(columnName, Attributes.ID_SUFFIX));
        } catch (final IllegalArgumentException e) {
            return managedType.getAttribute(columnName);
        }
    }
}
