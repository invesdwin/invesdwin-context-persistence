package de.invesdwin.context.persistence.jpa.api.dao;

import java.io.Serializable;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import de.invesdwin.context.persistence.jpa.api.query.QueryConfig;
import de.invesdwin.context.persistence.jpa.test.IClearAllTablesAware;

public interface IDao<E, PK extends Serializable> extends JpaRepository<E, PK>, IClearAllTablesAware {

    E findOneFast();

    E findOneFast(QueryConfig config);

    E findOneRandom();

    E findOne(E e);

    E findOne(E e, QueryConfig config);

    List<E> findAll(QueryConfig config);

    List<E> findAll(E example);

    List<E> findAll(E example, QueryConfig hints);

    long count(E example);

    long count(E example, QueryConfig config);

    void deleteAll(E example);

    boolean exists(E example);

    boolean isEmpty();

    PK extractId(E entity);

    E findOneById(PK id);

}
