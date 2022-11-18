package de.invesdwin.context.persistence.jpa.api.dao;

import java.lang.reflect.Method;
import java.util.Optional;

import javax.annotation.concurrent.Immutable;

import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.support.CrudMethodMetadata;
import org.springframework.data.jpa.repository.support.QueryHints;

import jakarta.persistence.LockModeType;

@Immutable
public final class DisabledCrudMethodMetadata implements CrudMethodMetadata {

    public static final DisabledCrudMethodMetadata INSTANCE = new DisabledCrudMethodMetadata();

    private DisabledCrudMethodMetadata() {}

    @Override
    public QueryHints getQueryHintsForCount() {
        return QueryHints.NoHints.INSTANCE;
    }

    @Override
    public QueryHints getQueryHints() {
        return QueryHints.NoHints.INSTANCE;
    }

    @Override
    public Method getMethod() {
        return null;
    }

    @Override
    public LockModeType getLockModeType() {
        return null;
    }

    @Override
    public Optional<EntityGraph> getEntityGraph() {
        return Optional.empty();
    }

    @Override
    public String getComment() {
        return null;
    }
}