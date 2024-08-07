package com.mzq.usage;

import org.apache.commons.lang3.RandomUtils;

import java.io.Serializable;
import java.time.LocalDate;

public class Employee implements Serializable {

    private static String[] deptNames = {"营销部", "运维部", "人力资源部", "研发部", "产品部", "销售部", "项目部", "测试部", "合规部", "法务部", "客户服务部", "生产部", "工程部", "安保部", "运力部"
            , "用户体验部", "商家服务部", "研发效能部", "行政部", "采销部", "安全部", "绿化部", "后勤保障部"};
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
        employee.setEmp_no("emp_" + RandomUtils.nextInt(1, 5000000));
        int deptNo = RandomUtils.nextInt(0, deptNames.length);
        employee.setDept_no("dept_" + deptNo);
        employee.setDept_name(deptNames[deptNo]);
        employee.setSex(new String[]{"male", "female"}[RandomUtils.nextInt(0, 2)]);
        employee.setEmp_name(buildFullName.fullName(employee.getSex()));
        employee.setSalary(RandomUtils.nextInt(3000, 9000));
        employee.setCreate_dt(LocalDate.now().minusDays(RandomUtils.nextInt(0, 61)).toString());
        return employee;
    }

    public static Employee[] generate(int cnt) {
        Employee[] array=new Employee[cnt];
        for (int i = 0; i < cnt; i++) {
            array[i] = generate();
        }
        return array;
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%s,%s,%s,%d%n", getEmp_no(), getEmp_name(), getDept_no(), getDept_name(), getSex(), getCreate_dt(), getSalary());
    }

    public static Employee from(String str) {
        String[] split = str.split(",");
        Employee employee = new Employee();
        employee.setEmp_no(split[0]);
        employee.setEmp_name(split[1]);
        employee.setDept_no(split[2]);
        employee.setDept_name(split[3]);
        employee.setSex(split[4]);
        employee.setCreate_dt(split[5]);
        employee.setSalary(Integer.valueOf(split[6]));
        return employee;
    }

    public static void main(String[] args) {
        System.out.println("1000".compareTo("100"));
    }
}
