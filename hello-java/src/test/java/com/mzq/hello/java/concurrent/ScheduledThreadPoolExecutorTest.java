package com.mzq.hello.java.concurrent;

import org.junit.Test;

import java.util.concurrent.*;

public class ScheduledThreadPoolExecutorTest {

    @Test
    public void testScheduleWithFixedDelay() {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        // 定期延时执行任务
        ScheduledFuture<?> scheduledFuture = scheduledExecutorService.scheduleWithFixedDelay(() -> {
            System.out.println("hello");
        }, 5, 3, TimeUnit.SECONDS);

        // Delayed.getDelay方法代表距离任务执行还有多长时间，返回结果为正数代表还有一定的时间来执行任务，而如果返回0或负数，代表任务的执行时间已经过去了，也就是说当前时间超过了计划要执行的时间
        // 注意：getDelay方法返回的不是任务还有多长时间完成，因此即使返回结果为负数，调用ScheduledFuture.get方法仍可能会阻塞，因为即使任务开始执行了，任务执行也需要一定的时间
        System.out.println(scheduledFuture.getDelay(TimeUnit.MILLISECONDS));

        Object s = null;
        try {
            // 如果提交的任务是定期循环执行，那么返回的ScheduledFuture对象是永远不会完成的，因此此时调用get方法，当前线程会无限期的阻塞
            s = scheduledFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        // 除非有其他线程中断当前线程的阻塞状态，否则一直执行不到这行代码
        System.out.println(s);
    }

    @Test
    public void testSchedule() {
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
        // 我们也可以使用schedule方法来仅定时执行一次任务
        ScheduledFuture<String> scheduledFuture = scheduledThreadPoolExecutor.schedule(() -> {
            return "hello";
        }, 5, TimeUnit.SECONDS);

        // 获取任务还有多长时间执行，如果任务还需要一段时间才能执行，就阻塞一下当前线程
        long delay = scheduledFuture.getDelay(TimeUnit.MILLISECONDS);
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        try {
            // 主线程尝试获取定时任务执行后的结果。相较于scheduleWithFixedDelay方法返回的ScheduledFuture，schedule方法返回的ScheduledFuture是会执行完成的，就是在定时任务执行完毕的时候
            String result = scheduledFuture.get();
            System.out.println(result);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
