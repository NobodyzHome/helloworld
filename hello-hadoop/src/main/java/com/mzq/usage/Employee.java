package com.mzq.usage;

import org.apache.commons.lang3.RandomUtils;

import java.time.LocalDate;

public class Employee {

    private static String[] deptNames = {"营销部", "运维部", "人力资源部", "研发部", "产品部", "销售部", "项目部", "测试部", "合规部", "法务部", "客户服务部"};

    private String emp_no;
    private String emp_name;
    private String dept_no;
    private String dept_name;
    private String sex;
    private String create_dt;
    private int salary;

    public String getEmp_no() {
        return emp_no;
    }

    public void setEmp_no(String emp_no) {
        this.emp_no = emp_no;
    }

    public String getEmp_name() {
        return emp_name;
    }

    public void setEmp_name(String emp_name) {
        this.emp_name = emp_name;
    }

    public String getDept_no() {
        return dept_no;
    }

    public void setDept_no(String dept_no) {
        this.dept_no = dept_no;
    }

    public String getDept_name() {
        return dept_name;
    }

    public void setDept_name(String dept_name) {
        this.dept_name = dept_name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getCreate_dt() {
        return create_dt;
    }

    public void setCreate_dt(String create_dt) {
        this.create_dt = create_dt;
    }

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    public static Employee generate() {
        BuildFullName buildFullName = new BuildFullName();
        Employee employee = new Employee();
        employee.setEmp_no("emp_" + RandomUtils.nextInt(1, 100));
        int deptNo = RandomUtils.nextInt(0, deptNames.length);
        employee.setDept_no("dept_" + deptNo);
        employee.setDept_name(deptNames[deptNo]);
        employee.setSex(new String[]{"male", "female"}[RandomUtils.nextInt(0, 2)]);
        employee.setEmp_name(buildFullName.fullName(employee.getSex()));
        employee.setSalary(RandomUtils.nextInt(3000, 5000));
        employee.setCreate_dt(LocalDate.parse("2020-03-05").plusMonths(RandomUtils.nextInt(1, 5)).plusDays(RandomUtils.nextInt(1, 90)).toString());
        return employee;
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%s,%s,%s,%d%n", getEmp_no(), getEmp_name(), getDept_no(), getDept_name(), getSex(), getCreate_dt(), getSalary());
    }
}
