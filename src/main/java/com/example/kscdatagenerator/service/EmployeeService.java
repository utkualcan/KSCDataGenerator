package com.example.kscdatagenerator.service;

import com.example.kscdatagenerator.entity.Employee;
import com.example.kscdatagenerator.repository.EmployeeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EmployeeService {

    @Autowired
    private EmployeeRepository employeeRepository;

    public List<Employee> findAllWithManagers() {
        return employeeRepository.findAllWithManagers();
    }

    public List<Employee> findAll() {
        return employeeRepository.findAll();
    }

    public Employee findById(Integer empno) {
        return employeeRepository.findById(empno).orElse(null);
    }

    public void save(Employee employee) {
        employeeRepository.save(employee);
    }

    public void deleteById(Integer empno) {
        employeeRepository.deleteById(empno);
    }
}
