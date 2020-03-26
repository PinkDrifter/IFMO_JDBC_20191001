package com.efimchick.ifmo.web.jdbc;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

public class SetMapperFactory {

    public SetMapper<Set<Employee>> employeesSetMapper() {

        return new SetMapper<Set<Employee>>() {
            @Override
            public Set<Employee> mapSet(ResultSet resultSet) {
                Set<Employee> employees = new HashSet<>();
                String sql = "select employee.* from employee";
                try {
                    while (resultSet.next()) employees.add(mapRowEmp(resultSet));
                    return employees;
                } catch (SQLException e) {
                    e.printStackTrace();
                } return null;
            }
        };
    }

    private Employee mapRowEmp(ResultSet resultSet) throws SQLException {
        Employee employee = new Employee(
                BigInteger.valueOf(resultSet.getInt("id")),
                new FullName(
                        resultSet.getString("firstName"),
                        resultSet.getString("lastName"),
                        resultSet.getString("middleName")
                ),
                Position.valueOf(resultSet.getString("position")),
                LocalDate.parse(resultSet.getString("hireDate")),
                resultSet.getBigDecimal("salary"),
                resultSet.getInt("manager")!=0 ? mapRowManager(resultSet) : null
        );
        return employee;
    }

    private Employee mapRowManager(ResultSet resultSet) throws SQLException {
        int manId = resultSet.getInt("manager");
        int curRow = resultSet.getRow();
        Employee manager = null;
        resultSet.beforeFirst();
        while (resultSet.next()) {
            if (resultSet.getInt("id") == manId) {
                manager = mapRowEmp(resultSet);
                break;
            }
        }
        resultSet.absolute(curRow);
        return manager;
    }
}
