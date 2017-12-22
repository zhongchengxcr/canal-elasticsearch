package test.syn;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(2);


        TestFuture testFuture = new TestFuture(() -> {
            Thread.sleep(4000);
            return "zhong";
        });

        executorService.submit(testFuture);

        System.out.printf("-------");

        System.out.printf(testFuture.get());

        System.out.printf("asdasda");

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
