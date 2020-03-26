package com.efimchick.ifmo.web.jdbc.dao;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class DaoFactory {
    public EmployeeDao employeeDAO() {
        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                String sql = "select * from employee where department=?";
                List<Employee> employees = new LinkedList<>();
                try {
                    PreparedStatement preparedStatement = ConnectionSource.instance().createConnection().prepareStatement(sql);
                    preparedStatement.setInt(1, department.getId().intValue());
                    ResultSet resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                        employees.add(mapRowEmp(resultSet, false));
                    }
                    return employees;
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                String sql = "select * from employee where manager=?";
                List<Employee> employees = new LinkedList<>();
                try {
                    PreparedStatement preparedStatement = ConnectionSource.instance().createConnection().prepareStatement(sql);
                    preparedStatement.setInt(1, employee.getId().intValue());
                    ResultSet resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                        employees.add(mapRowEmp(resultSet, false));
                    }
                    return employees;
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                String sql ="select * from employee where id=?";
                try {
                    PreparedStatement preparedStatement = ConnectionSource.instance().createConnection().prepareStatement(sql);
                    preparedStatement.setInt(1, Integer.parseInt(String.valueOf(Id)));
                    ResultSet resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                        return Optional.of(mapRowEmp(resultSet, true));
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return Optional.empty();
            }

            @Override
            public List<Employee> getAll() {
                String sql = "select * from employee";
                List<Employee> employees = new LinkedList<>();
                try {
                    PreparedStatement preparedStatement = ConnectionSource.instance().createConnection().prepareStatement(sql);
                    ResultSet resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                        employees.add(mapRowEmp(resultSet, true));
                    }
                    return employees;
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public Employee save(Employee employee) {
                String sql = "insert into employee values (?,?,?,?,?,?,?,?,?)";
                try {
                    PreparedStatement statement = ConnectionSource.instance().createConnection().prepareStatement(sql);
                    statement.setInt(1, employee.getId().intValue());
                    statement.setString(2, employee.getFullName().getFirstName());
                    statement.setString(3, employee.getFullName().getLastName());
                    statement.setString(4, employee.getFullName().getMiddleName());
                    statement.setString(5, employee.getPosition().toString());
                    statement.setInt(6, employee.getManagerId().intValue());
                    statement.setDate(7, Date.valueOf(employee.getHired()));
                    statement.setDouble(8, employee.getSalary().doubleValue());
                    statement.setInt(9, employee.getDepartmentId().intValue());
                    statement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return employee;
            }

            @Override
            public void delete(Employee employee) {
                String query = "delete from employee where id=?";
                try {
                    PreparedStatement statement = ConnectionSource.instance().createConnection().prepareStatement(query);
                    statement.setInt(1, employee.getId().intValue());
                    statement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private Employee mapRowEmp(ResultSet resultSet, boolean optional) throws SQLException {
        return new Employee(
                BigInteger.valueOf(resultSet.getInt("id")),
                new FullName(
                        resultSet.getString("firstName"),
                        resultSet.getString("lastName"),
                        resultSet.getString("middleName")
                ),
                Position.valueOf(resultSet.getString("position")),
                LocalDate.parse(resultSet.getString("hireDate")),
                resultSet.getBigDecimal("salary"),
                (!optional) ? (resultSet.getObject("manager") != null ? BigInteger.valueOf(resultSet.getInt("manager")) : null) : (resultSet.getObject("manager") != null ? BigInteger.valueOf(resultSet.getInt("manager")) : BigInteger.ZERO),
                (!optional) ? (resultSet.getObject("department") != null ? BigInteger.valueOf(resultSet.getInt("department")) : null) : (resultSet.getObject("department") != null ? BigInteger.valueOf(resultSet.getInt("department")) : BigInteger.ZERO)
        );
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                String sql ="select * from department where id=?";
                try {
                    PreparedStatement preparedStatement = ConnectionSource.instance().createConnection().prepareStatement(sql);
                    preparedStatement.setInt(1, Integer.parseInt(String.valueOf(Id)));
                    ResultSet resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                        return Optional.of(mapRowDep(resultSet));
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return Optional.empty();
            }

            @Override
            public List<Department> getAll() {
                String sql ="select * from department";
                List<Department> departments = new LinkedList<>();
                try {
                    PreparedStatement preparedStatement = ConnectionSource.instance().createConnection().prepareStatement(sql);
                    ResultSet resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                        departments.add(mapRowDep(resultSet));
                    }
                    return departments;
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public Department save(Department department) {
                String query;
                PreparedStatement statement;
                try {
                    if (getById(BigInteger.valueOf(department.getId().intValue())).isPresent()) {
                        query = "update department set name=?, location=? where id=?";
                        statement = ConnectionSource.instance().createConnection().prepareStatement(query);
                        statement.setString(1, department.getName());
                        statement.setString(2, department.getLocation());
                        statement.setString(3, String.valueOf(department.getId().intValue()));
                        statement.executeUpdate();
                    } else {
                        query = "insert into department values (?,?,?)";
                        statement = ConnectionSource.instance().createConnection().prepareStatement(query);
                        statement.setString(1, String.valueOf(department.getId().intValue()));
                        statement.setString(2, department.getName());
                        statement.setString(3, department.getLocation());
                        statement.executeUpdate();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return department;
            }

            @Override
            public void delete(Department department) {
                String query = "delete from department where id=?";
                try {
                    PreparedStatement statement = ConnectionSource.instance().createConnection().prepareStatement(query);
                    statement.setInt(1, department.getId().intValue());
                    statement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private Department mapRowDep(ResultSet resultSet) throws SQLException {
        return new Department(
                BigInteger.valueOf(resultSet.getInt("id")),
                resultSet.getString("name"),
                resultSet.getString("location")
        );
    }
}
