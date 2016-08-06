package de.invesdwin.context.persistence.jpa.api.dao.entity;

import javax.annotation.concurrent.NotThreadSafe;
import javax.persistence.MappedSuperclass;

import de.invesdwin.context.persistence.jpa.api.dao.entity.sequence.AUnversionedEntityWithSequence;

/**
 * An Entity is the same as a table row and is persistent as one. These are JPA-Annotated classes that do not contain
 * any logic, but just data and verification.
 * 
 * The lifecycle status fields may be only manipulated by infrastructure classes, that's why the setters for deleted and
 * persistent are package private.
 * 
 * @author subes
 * 
 */
@MappedSuperclass
@NotThreadSafe
public abstract class AUnversionedEntity extends AUnversionedEntityWithSequence {

}