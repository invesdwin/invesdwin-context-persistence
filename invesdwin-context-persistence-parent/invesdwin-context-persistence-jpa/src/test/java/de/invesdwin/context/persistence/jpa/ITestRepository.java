package de.invesdwin.context.persistence.jpa;

import org.springframework.data.jpa.repository.JpaRepository;

import de.invesdwin.context.persistence.jpa.complex.TestEntity;

public interface ITestRepository extends JpaRepository<TestEntity, Long> {

    TestEntity findTestByName(String name);

}
