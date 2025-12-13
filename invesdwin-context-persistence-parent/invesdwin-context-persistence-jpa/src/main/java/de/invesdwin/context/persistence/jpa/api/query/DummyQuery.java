package de.invesdwin.context.persistence.jpa.api.query;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.collections.Collections;
import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.Query;
import jakarta.persistence.TemporalType;

@NotThreadSafe
public class DummyQuery implements Query {

    @Override
    public List<?> getResultList() {
        return Collections.emptyList();
    }

    @Override
    public Object getSingleResult() {
        return null;
    }

    @Override
    public int executeUpdate() {
        return 0;
    }

    @Override
    public Query setMaxResults(final int maxResult) {
        return this;
    }

    @Override
    public int getMaxResults() {
        return 0;
    }

    @Override
    public Query setFirstResult(final int startPosition) {
        return this;
    }

    @Override
    public int getFirstResult() {
        return 0;
    }

    @Override
    public Query setHint(final String hintName, final Object value) {
        return this;
    }

    @Override
    public Map<String, Object> getHints() {
        return Collections.emptyMap();
    }

    @Override
    public <T> Query setParameter(final Parameter<T> param, final T value) {
        return this;
    }

    @Override
    public Query setParameter(final Parameter<Calendar> param, final Calendar value, final TemporalType temporalType) {
        return this;
    }

    @Override
    public Query setParameter(final Parameter<Date> param, final Date value, final TemporalType temporalType) {
        return this;
    }

    @Override
    public Query setParameter(final String name, final Object value) {
        return this;
    }

    @Override
    public Query setParameter(final String name, final Calendar value, final TemporalType temporalType) {
        return this;
    }

    @Override
    public Query setParameter(final String name, final Date value, final TemporalType temporalType) {
        return this;
    }

    @Override
    public Query setParameter(final int position, final Object value) {
        return this;
    }

    @Override
    public Query setParameter(final int position, final Calendar value, final TemporalType temporalType) {
        return this;
    }

    @Override
    public Query setParameter(final int position, final Date value, final TemporalType temporalType) {
        return this;
    }

    @Override
    public Set<Parameter<?>> getParameters() {
        return Collections.emptySet();
    }

    @Override
    public Parameter<?> getParameter(final String name) {
        return null;
    }

    @Override
    public <T> Parameter<T> getParameter(final String name, final Class<T> type) {
        return null;
    }

    @Override
    public Parameter<?> getParameter(final int position) {
        return null;
    }

    @Override
    public <T> Parameter<T> getParameter(final int position, final Class<T> type) {
        return null;
    }

    @Override
    public boolean isBound(final Parameter<?> param) {
        return false;
    }

    @Override
    public <T> T getParameterValue(final Parameter<T> param) {
        return (T) null;
    }

    @Override
    public Object getParameterValue(final String name) {
        return null;
    }

    @Override
    public Object getParameterValue(final int position) {
        return null;
    }

    @Override
    public Query setFlushMode(final FlushModeType flushMode) {
        return this;
    }

    @Override
    public FlushModeType getFlushMode() {
        return null;
    }

    @Override
    public Query setLockMode(final LockModeType lockMode) {
        return this;
    }

    @Override
    public LockModeType getLockMode() {
        return null;
    }

    @Override
    public <T> T unwrap(final Class<T> cls) {
        return null;
    }

    @Override
    public Integer getTimeout() {
        return null;
    }

    @Override
    public Query setTimeout(final Integer arg0) {
        return this;
    }

    @Override
    public CacheRetrieveMode getCacheRetrieveMode() {
        return null;
    }

    @Override
    public Query setCacheRetrieveMode(final CacheRetrieveMode arg0) {
        return this;
    }

    @Override
    public CacheStoreMode getCacheStoreMode() {
        return null;
    }

    @Override
    public Query setCacheStoreMode(final CacheStoreMode arg0) {
        return this;
    }

    @Override
    public Object getSingleResultOrNull() {
        return null;
    }

}
