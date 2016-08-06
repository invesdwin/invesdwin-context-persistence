package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.Immutable;

@Immutable
public enum ConnectionAutoSchema {
    VALIDATE,
    UPDATE,
    CREATE,
    CREATE_DROP;
}
