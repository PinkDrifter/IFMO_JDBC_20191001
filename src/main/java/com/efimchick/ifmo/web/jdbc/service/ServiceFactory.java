package com.efimchick.ifmo.web.jdbc.service;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;

public class ServiceFactory {

    public EmployeeService employeeService(){
        return new EmployeeService() {
            @Override
            public List<Employee> getAllSortByHireDate(Paging paging) {
                String sql = "select * from employee order by hireDate limit " + paging.itemPerPage + " offset " + (paging.page-1)*paging.itemPerPage;
                try {
                    return mapSetEmployeesWithManager(sql, true);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getAllSortByLastname(Paging paging) {
                String sql = "select * from employee order by lastName limit " + paging.itemPerPage + " offset " + (paging.page-1)*paging.itemPerPage;
                try {
                    return mapSetEmployeesWithManager(sql, true);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getAllSortBySalary(Paging paging) {
                String sql = "select * from employee order by salary limit " + paging.itemPerPage + " offset " + (paging.page-1)*paging.itemPerPage;
                try {
                    return mapSetEmployeesWithManager(sql, true);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getAllSortByDepartmentNameAndLastname(Paging paging) {
                String sql = "select * from employee left join department on employee.department = department.id order by department.name, lastName limit " + paging.itemPerPage + " offset " + (paging.page-1)*paging.itemPerPage;
                try {
                    return mapSetEmployeesWithManager(sql, true);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByDepartmentSortByHireDate(Department department, Paging paging) {
                String sql = "select * from employee left join department on employee.department = department.id where department="+ department.getId().toString() + " order by hireDate limit " + paging.itemPerPage + " offset " + (paging.page-1)*paging.itemPerPage;
                try {
                    return mapSetEmployeesWithManager(sql, true);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
                String sql = "select * from employee left join department on employee.department = department.id where department="+ department.getId().toString() + " order by salary limit " + paging.itemPerPage + " offset " + (paging.page-1)*paging.itemPerPage;
                try {
                    return mapSetEmployeesWithManager(sql, true);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
                String sql = "select * from employee left join department on employee.department = department.id where department="+ department.getId().toString() + " order by lastName limit " + paging.itemPerPage + " offset " + (paging.page-1)*paging.itemPerPage;
                try {
                    return mapSetEmployeesWithManager(sql, true);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
                String sql = "select * from employee where manager="+ manager.getId().toString() + " order by lastName limit " + paging.itemPerPage + " offset " + (paging.page-1)*paging.itemPerPage;
                try {
                    return mapSetEmployeesWithManager(sql, true);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
                String sql = "select * from employee where manager="+ manager.getId().toString() + " order by hireDate limit " + paging.itemPerPage + " offset " + (paging.page-1)*paging.itemPerPage;
                try {
                    return mapSetEmployeesWithManager(sql, true);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
                String sql = "select * from employee where manager="+ manager.getId().toString() + " order by salary limit " + paging.itemPerPage + " offset " + (paging.page-1)*paging.itemPerPage;
                try {
                    return mapSetEmployeesWithManager(sql, true);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;

            }

            @Override
            public Employee getWithDepartmentAndFullManagerChain(Employee employee) {
                String sql = "select * from employee where id="+ employee.getId().toString();
                try {
                    return mapSetEmployeesWithChain(sql).get(0);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public Employee getTopNthBySalaryByDepartment(int salaryRank, Department department) {
                String sql = "select * from employee where department=" + department.getId().toString() + " order by salary desc";
                try {
                    return mapSetEmployeesWithManager(sql, true).get(salaryRank-1);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
    }

    private List<Employee> mapSetEmployeesWithManager(String sql, boolean manager_is_null) throws SQLException {
        List<Employee> employees = new LinkedList<>();
        ResultSet resultSet = ConnectionSource.instance().createConnection().createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE).executeQuery(sql);
        while (resultSet.next()) employees.add(mapRowEmployeeWithManager(resultSet, manager_is_null));
        return employees;
    }

    private List<Employee> mapSetEmployeesWithChain(String sql) throws SQLException {
        List<Employee> employees = new LinkedList<>();
        ResultSet resultSet = ConnectionSource.instance().createConnection().createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE).executeQuery(sql);
        while (resultSet.next()) employees.add(mapRowEmployeeWithChain(resultSet));
        return employees;
    }

    private Employee mapRowEmployeeWithManager(ResultSet resultSet, boolean include_manager) throws SQLException {
        return getEmployee(resultSet, resultSet.getObject("manager") != null && include_manager, mapSetEmployeesWithManager("select * from employee where id=" + resultSet.getString("manager"), false));
    }

    public Employee mapRowEmployeeWithChain(ResultSet resultSet) throws SQLException {
        return getEmployee(resultSet, resultSet.getObject("manager") != null, mapSetEmployeesWithChain("select * from employee where id=" + resultSet.getString("manager")));
    }

    private Employee getEmployee(ResultSet resultSet, boolean manager2, List<Employee> manager3) throws SQLException {
        BigInteger id = new BigInteger(resultSet.getString("id"));
        FullName fullName = new FullName(
                resultSet.getString("firstName"),
                resultSet.getString("lastName"),
                resultSet.getString("middleName")
        );
        Position position = Position.valueOf(resultSet.getString("position"));
        LocalDate date = LocalDate.parse(resultSet.getString("hireDate"));
        BigDecimal salary = resultSet.getBigDecimal("salary");
        Employee manager = (manager2) ? manager3.get(0) : null;
        Department department = (resultSet.getObject("department") != null) ? mapRowDep(BigInteger.valueOf(resultSet.getInt("department"))) : null;
        return new Employee(id, fullName, position, date, salary, manager, department);
    }

    public Department mapRowDep(BigInteger id) {
        try {
            ResultSet resultSet = ConnectionSource.instance().createConnection().createStatement().executeQuery("SELECT * FROM DEPARTMENT where id=" + id);
            return (resultSet.next()) ? new Department(id, resultSet.getString("name"), resultSet.getString("location")) : null;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
}
