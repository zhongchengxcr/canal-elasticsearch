package test.syn;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/04 下午2:19
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class Lock {

    public static void get() {
        LockSupport.park();
    }


    public static void main(String[] args) {

        Student student = new Student("ZHONGC",12);
        List<Student> students = new ArrayList<>();

        students.add(student);
        student = new Student("asd",18);
        students.add(student);

    }


    public static class Student {


        public Student(String name, int age) {
            this.name = name;
            this.age = age;
        }

        private String name;

        private int age;

        public String getName() {
            return name;
        }

        public Student setName(String name) {
            this.name = name;
            return this;
        }

        public int getAge() {
            return age;
        }

        public Student setAge(int age) {
            this.age = age;
            return this;
        }
    }
}
