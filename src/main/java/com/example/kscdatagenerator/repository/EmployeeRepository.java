package com.example.kscdatagenerator.repository;

import com.example.kscdatagenerator.entity.Employee;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface EmployeeRepository extends JpaRepository<Employee, Integer> {
    @Query("SELECT e FROM Employee e LEFT JOIN Employee m ON e.mgr = m.empno")
    List<Employee> findAllWithManagers();
}
