package de.invesdwin.context.persistence.jpa.api.dao;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.jpa.api.dao.entity.IEntity;
import de.invesdwin.context.persistence.jpa.api.query.DummyQuery;
import de.invesdwin.context.persistence.jpa.api.query.QueryConfig;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.reflection.Reflections;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import jakarta.persistence.metamodel.Attribute;
import jakarta.persistence.metamodel.EntityType;

@Immutable
public class QueryByExampleHelper<E, PK extends Serializable> {

    private final IDao<E, PK> dao;

    public QueryByExampleHelper(final IDao<E, PK> dao) {
        this.dao = dao;
    }

    private void assertEntityExampleWithoutId(final E example) {
        Assertions.assertThat(dao.extractId(example))
                .as("The id must be null for a QueryByExample. Maybe you would rather use the find/delete method directly?")
                .isNull();
    }

    /**
     * When and=false, all criteria are combined with OR, else AND is used.
     * 
     * @see <a href="http://stackoverflow.com/questions/2880209/jpa-findbyexample">Source</a>
     */
    public TypedQuery<E> queryByExample(final String persistenceUnitName, final EntityManager em,
            final Class<E> genericType, final E example, final boolean and, final QueryConfig config) {
        assertEntityExampleWithoutId(example);
        final CriteriaBuilder cb = em.getCriteriaBuilder();
        final CriteriaQuery<E> cq = cb.createQuery(genericType);
        final Root<E> r = cq.from(genericType);
        Predicate p = cb.conjunction();
        final EntityType<E> et = em.getMetamodel().entity(genericType);
        final Set<Attribute<? super E, ?>> attrs = et.getAttributes();
        boolean firstField = true;
        for (final Attribute<? super E, ?> attr : attrs) {
            final String name = attr.getName();
            final String javaName = attr.getJavaMember().getName();
            final Field f = Reflections.findField(genericType, javaName);
            Reflections.makeAccessible(f);
            final Object value = Reflections.getField(f, example);
            if (value != null) {
                final Predicate pred = cb.equal(r.get(name), value);
                if (and || firstField) {
                    p = cb.and(p, pred);
                } else {
                    p = cb.or(p, pred);
                }
                firstField = false;
            }
        }
        cq.select(r).where(p);
        final TypedQuery<E> query = em.createQuery(cq);
        QueryConfig.configure(persistenceUnitName, query, config);
        return query;
    }

    public Query countByExample(final EntityManager em, final Class<E> genericType, final E example,
            final boolean and) {
        return xByExample(em, "SELECT COUNT(*)", genericType, example, and);
    }

    public Query deleteByExample(final EntityManager em, final Class<E> genericType, final E example,
            final boolean and) {
        return xByExample(em, "DELETE", genericType, example, and);
    }

    private Query xByExample(final EntityManager em, final String queryStart, final Class<E> genericType,
            final E example, final boolean and) {
        assertEntityExampleWithoutId(example);
        final StringBuilder sb = new StringBuilder(queryStart);
        sb.append(" FROM ");
        sb.append(extractEntityName(genericType));
        sb.append(" e WHERE 1 = 1");
        final Map<String, Object> params = new HashMap<String, Object>();
        final EntityType<E> et = em.getMetamodel().entity(genericType);
        final Set<Attribute<? super E, ?>> attrs = et.getAttributes();
        boolean firstField = true;
        for (final Attribute<? super E, ?> attr : attrs) {
            final String name = attr.getName();
            final String javaName = attr.getJavaMember().getName();
            final Field f = Reflections.findField(genericType, javaName);
            Reflections.makeAccessible(f);
            final Object value = Reflections.getField(f, example);
            if (value != null) {
                params.put(name, value);
                if (and || firstField) {
                    sb.append(" AND ");
                } else {
                    sb.append(" OR ");
                }
                sb.append("e.");
                sb.append(name);
                sb.append(" = :");
                sb.append(name);
                firstField = false;
            }
        }
        final Query query = em.createQuery(sb.toString());
        for (final Entry<String, Object> param : params.entrySet()) {
            query.setParameter(param.getKey(), param.getValue());
        }
        return query;
    }

    protected String extractEntityName(final Class<E> genericType) {
        return genericType.getName();
    }

    public Query deleteByIds(final EntityManager em, final Class<E> genericType, final Iterable<Long> ids) {
        if (ids == null || !ids.iterator().hasNext()) {
            return new DummyQuery();
        }
        final StringBuilder sb = new StringBuilder("DELETE FROM ");
        sb.append(extractEntityName(genericType));
        sb.append(" e WHERE ");
        boolean first = true;
        for (final Long id : ids) {
            if (!first) {
                sb.append(" OR ");
            }
            sb.append("e.");
            sb.append(IEntity.ID_COLUMN_NAME);
            sb.append(" = ");
            sb.append(id);
            first = false;
        }
        final Query query = em.createQuery(sb.toString());
        return query;
    }

}
