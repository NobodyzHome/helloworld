package com.mzq.hello.java.concurrent;

import org.junit.Test;

import java.util.Objects;
import java.util.concurrent.*;

public class CompletableFutureTest {

    /**
     * Future的核心思想是：主线程想让子线程去做些事情，并且需要获取子线程做完事情的结果，那么子线程就给主线程开了张空头支票Future
     * 主线程拿着这张空头支票，在需要获取子线程结果时，就调用这个Future的get方法获取子线程的执行结果。如果此时子线程已经执行完了，就可以顺序拿到子线程的处理结果。但如果子线程没有执行完，则主线程会被阻塞住，等待子线程执行完成。
     * 而与此同时，子线程在处理完毕后，就把这个空头支票Future设置为已处理完成，此后主线程调用这个Future的get方法，就可以获取子线程的执行结果了
     * <p>
     * Future的一个不方便的点在于：用户线程在获取到子线程给的Future对象后，需要自己来判断这个Future是否执行完成了。我们在实际应用中有一种需求是期望在这个Future执行完成（执行成功或异常）后，来执行对应的后续处理任务，而不是
     * 要主线程一直来手动的判断Future是否完成了，完成以后再做一些其他的处理。
     * 这也是jdk8的CompleteableFuture的用意，让用户线程只需要布置后续的处理任务即可，不需要关心这个Future是否已经完成了，CompleteableFuture内部会在合适的时机进行后续任务的执行。
     */
    @Test
    public void testFuture() {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        // 主线程让子线程去执行一个任务，然后返回一个空头支票future，此时任务有可能还没有执行完
        Future<String> future = executorService.submit(() -> {
            // 模拟子线程执行了5秒才计算出结果
            Thread.sleep(5000);
            return "hello world";
        });

        String result = null;
        // 用户线程需要主动遍历，判断future对应的任务是否执行完了
        do {
            try {
                result = future.get(800, TimeUnit.MILLISECONDS);
                // 用户线程需要等获取到Future的结果后才能进行后续相关的任务，在这里假设后续任务就是把Future的结果包装一下然后输出出来
                System.out.println("end:" + result);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace();
            }
        } while (Objects.isNull(result));
    }


    /**
     * 我们把上面使用Future不方便的地方使用CompletableFuture进行改造，发现就少了很多用户线程等待Future执行完成的代码，而是由CompletableFuture自己
     * 来判断应该何时执行任务。
     */
    @Test
    public void testUpdateFuture() {
        /*
            CompletableFuture.supplyAsync方法会：
            1.创建一个CompletableFuture对象
            2.把传入的Supplier对象封装成一个任务，把该任务提交给另一个线程来执行
            3.把创建的CompletableFuture对象返回给用户线程
            此时用户线程收到的CompletableFuture对象是未完成的，待另一个线程中的Supplier对象执行完毕后，会给该CompletableFuture对象赋值成已完成

            因此CompletableFuture.supplyAsync方法集成了：创建CompletableFuture对象、异步提交任务、当任务执行完成后，为CompletableFuture对象赋值完成状态的功能。
            所以：使用CompletableFuture.supplyAsync的话，就只需要给出如何获取CompletableFuture的结果就可以了，创建CompletableFuture对象和给CompletableFuture对象赋值完成状态都由supplyAsync内部去做了。
        */
        CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync(() -> {
            // 模拟子线程执行了5秒才计算出结果
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "hello world";
        });

        // 以下这个代码和上面CompletableFuture.supplyAsync方法的处理内容一致，可以看到使用CompletableFuture.supplyAsync更简便一些
        CompletableFuture<String> completableFuture1 = new CompletableFuture<>();
        new Thread(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            completableFuture1.complete("hello world");
        }).start();

        // 执行到这里时，CompletableFuture.supplyAsync返回的CompletableFuture对象可能还没有完成，不过没有关系，我们先搭建CompletableFuture链条，安排后续任务
        // 不论completableFuture是否完成，只需要告诉他后续执行的任务即可，不需要用户手动判断Future是否执行完。这是对比Future的最方便的地方
        // 上面例子中，还得使用遍历，一次次地判断Future是否执行完，在获取完成Future的结果后才能继续进行后续处理
        completableFuture.thenAccept(str -> System.out.println("end:" + str));
        System.out.println("main end");
    }

    /**
     * CompletableFuture的核心思想是：为了达到当一个异步任务处理完成后，自动处理后续任务的目的。我搭建一个CompletableFuture链条，链条上的每个CompletableFuture代表了一个任务。
     * 等链条上的第一个CompletableFuture完成后，会自动执行链条上后续的CompletableFuture对应的任务。让我们不再关心Future是否已完成，只需要布置后续操作任务即可。
     * <p>
     * CompletableFuture future1=completableFuture.thenApply(str -> str+"world")代表的意义是：
     * 需要在completableFuture这个任务完成后，执行任务str -> str+"world"，然后把一个空头支票future1（它有可能还没完成）返回给用户
     * <p>
     * 我们在调用completableFuture.thenApply方法时，实际上代表创建一个新的CompletableFuture，这个CompletableFuture对象对应的任务是在completableFuture有结果后，执行str -> return str + "world";方法，
     * 也就是形成了这样一个CompletableFuture链
     * 用户自定义任务            =》     str -> return str + "world";
     * completableFuture       =》             future1
     * <p>
     * 下面这些代码，实际上就是形成了一个CompletableFuture的链表，链表中每一个CompletableFuture代表了一个任务
     * 用户自定义任务            =》     str -> return str + "world";        =》      str -> "test:" + str;       =》          str -> System.out.println(str);
     * completableFuture       =》             future1                     =》           future2                =》                   future3
     * <p>
     * 上面我们说了completableFuture.thenApply(str -> return str + "world")这行代码的作用就是建立一个completableFuture和future1之间的CompletableFuture链
     * 1.如果在创建链时，completableFuture就已经完成了（有其他线程或当前线程调用过completableFuture的complete方法了），那么thenApply的执行内容，这些处理都是在【执行thenApply方法的当前线程】中串行执行的:
     * a) 创建一个CompletableFuture对象future1
     * b) 在中执行future1这个对象对应的任务str -> return str + "world"
     * c) 把任务返回结果赋值给future1
     * d) 返回future1，thenApply方法就执行结束了
     * <p>
     * 2.如果在创建链时，completableFuture没有完成，那么thenApply的执行内容:
     * a) 创建一个CompletableFuture对象future1
     * b) 把future1对象返回，thenApply方法就执行结束了
     * <p>
     * 总结：决定completableFuture.thenApply(str -> return str + "world")怎么处理的因素是completableFuture是否已经处理完成
     * <p>
     * 如果在completableFuture.thenApply(str -> return str + "world")时，completableFuture没有完成，那么如何触发str -> return str + "world"任务的执行？
     * 答案就是在触发completableFuture完成时，这里的完成有可能是执行成功，也有可能是执行异常。因此也就是调用completableFuture.complete、completableFuture.completeExceptionally方法时。
     * 在completableFuture.complete方法执行时，这些处理都是在【执行complete方法的当前线程】中串行执行的:
     * a) 把当前completableFuture设置成完成状态
     * b) 发现和completableFuture有相连的CompletableFuture对象future1，执行future1对应的任务str -> return str + "world"，执行完毕后把结果赋值给future1
     * c) 未发现和future1有相连的CompletableFuture对象，处理结束
     **/
    @Test
    public void testCompleteBeforeAssign() {
        CompletableFuture<String> completableFuture = new CompletableFuture<>();
        // 我们让这个Future变成完成状态，且Future完成后的结果内容是hello
        // 通常是在另外一个线程中，异步执行完操作后，把主线程的CompletableFuture对象设置为已完成状态
        completableFuture.complete("hello");

        // 由于completableFuture已经完成了，那么在调用thenApply方法时，以下处理内容都是在【当前线程】处理的：
        // 1.创建一个新的CompletableFuture对象future1
        // 2.继续执行str=>str + "world"
        // 3.把任务执行的结果赋值给future1，
        // 4.把future1返回，thenApply方法就结束了
        CompletableFuture<String> future1 = completableFuture.thenApply(str -> {
            return str + "world";
        });
        // 由于future1已经完成了，那么调用thenApply方法执行的内容：1.创建CompletableFuture对象future2 2.执行任务str -> "test:" + str; 3.把执行结果赋值给future2 4.把future2返回
        CompletableFuture<String> future2 = future1.thenApply(str -> {
            return "test:" + str;
        });
        // 依此类推，返回的future3已经完成了
        CompletableFuture<Void> future3 = future2.thenAccept(str -> {
            System.out.println(str);
        });
        future3.thenRun(() -> System.out.println("execute end"));
        System.out.println("end");
    }

    /**
     * 总结，触发CompletableFuture链条的每一个CompletableFuture变成完成状态的两个时机：
     * 1.使用thenApply、thenAccept等方法搭建链条时。前提是搭建链条时，链条上的第一个CompletableFuture就已经完成了
     * 2.对链条中第一个CompletableFuture调用complete、completeExceptionally等完结方法
     */
    @Test
    public void testCompleteAfterAssign() {
        CompletableFuture<String> completableFuture = new CompletableFuture<>();
        // 上面我们说了，completableFuture.thenApply(...).thenApply(...).thenAccept(...)实际上是形成了一个CompletableFuture链，而且由于completableFuture没有完成，那么由它产生的后续future也都是没有完成的状态
        // 也就是future1、future2、future3都是没有完成的，那么何时处罚这个链表的每一个CompletableFuture对象变成完成状态呢？那就是在后续调用completableFuture.complete方法时
        CompletableFuture<String> future1 = completableFuture.thenApply(str -> {
            return "hello" + str;
        });
        CompletableFuture<String> future2 = future1.thenApply(str -> {
            return "test:" + str;
        });
        CompletableFuture<Void> future3 = future2.thenAccept(str -> {
            System.out.println(str);
        });

        /*
         * completableFuture.complete的处理内容，这些操作都是在【当前线程】同步执行的：
         * 1.把自身赋值成已完成状态
         * 2.发现和completableFuture有关联的future1，执行future1对应的任务str->"hello"+str，然后把结果赋值给future1
         * 3.发现和future1有关联的future2，执行future2对应的任务str->"test"+str，然后把结果赋值给future2
         * 4.发现和future2有关联的future3，执行future3对应的任务str->System.out.println(str);，然后把结果赋值给future3
         * 5.未发现和future3有关联的，completableFuture.complete方法处理结束
         * 也就是当调用completableFuture.complete方法时，它会尝试调用CompletableFuture链中的每一个任务，并把任务结果赋值给对应的CompletableFuture对象
         */
        completableFuture.complete("world");
        System.out.println("test");
    }

    /**
     * 关于异步的方法，例如completableFuture.thenApplyAsync(str->str+"world")，无论completableFuture是否执行完成，方法都会直接返回一个新的CompletableFuture
     * 对象，并且在【用户线程】是不执行任务str->str+"world"的。
     * 只不过如果completableFuture已完成，那么会新提出一个线程来执行任务str->str+"world"，而如果completableFuture未完成，则不会有该操作。
     */
    @Test
    public void testCompleteBeforeAssignAsync() {
        CompletableFuture<String> completableFuture = new CompletableFuture<>();
        completableFuture.complete("success");

        // 无论completableFuture是否执行完成，completableFuture.thenApplyAsync都会创建一个新的CompletableFuture对象并返回。
        // 只不过completableFuture如果已经完成，会在新的线程中执行任务str -> str + "world"，并把执行结果赋值给新的CompletableFuture对象
        CompletableFuture<String> completableFuture1 = completableFuture.thenApplyAsync(str -> {
            System.out.printf("[%s]received:%s%n", Thread.currentThread().getName(), str);
            return str + "world";
        });
        // 此时有可能completableFuture1还没有完成，那么该方法只是返回一个新创建的CompletableFuture对象
        CompletableFuture<String> completableFuture2 = completableFuture1.thenApplyAsync(str -> {
            System.out.printf("[%s]received:%s%n", Thread.currentThread().getName(), str);
            return "test:" + str;
        });
        CompletableFuture<Void> completableFuture3 = completableFuture2.thenAcceptAsync(str -> {
            System.out.printf("[%s]received:%s%n", Thread.currentThread().getName(), str);
            System.out.println(str);
        });
        CompletableFuture<Void> completableFuture4 = completableFuture3.thenRunAsync(() -> System.out.printf("[%s]received:%s%n", Thread.currentThread().getName(), "end"));

        System.out.println("test");
    }

    /**
     * 总体来说，同步和异步的处理的一致的，只不过同步的话是在用户线程执行任务，而异步的话是在其他线程中。
     * 同步的方式，用户线程需要等待在CompletableFuture.thenApply()、CompletableFuture.complete()时后续任务处理完成，才能结束处理，让用户线程执行后续处理
     * 异步的方式，用户线程不需要等待后续任务处理完成，thenApplyAsync方法很快的执行完毕，好让用户线程进行后续处理
     * <p>
     * 这两种方式各有利弊：
     * a) 如果主线程希望获取到后续任务的处理结果后才能继续执行，并且对用户线程的处理时长没有特别敏感的要求，我们就可以使用同步的方式
     * b) 如果主线程不关心后续任务的处理结果，我们就可以使用异步的方式，减少主线程的处理时长
     */
    @Test
    public void testCompleteAfterAssignAsync() {
        CompletableFuture<String> completableFuture = new CompletableFuture<>();
        CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
        new Thread(() -> {
            try {
                // 先让主线程把CompletableFuture链串起来，然后子线程再调用completableFuture.complete方法
                cyclicBarrier.await();
                // 在执行complete方法时，也是会判断该completableFuture是否有链接的CompletableFuture，如果有的话，就会执行对应的任务。只不过是提出一个线程来执行执行，而不是在调用complete方法的线程中执行，减少了complete方法的阻塞时间
                completableFuture.complete("hello");
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
        }).start();

        CompletableFuture<String> completableFuture1 = completableFuture.thenApplyAsync(str -> str + "world");
        CompletableFuture<String> completableFuture2 = completableFuture1.thenApplyAsync(str -> "test:" + str);
        CompletableFuture<Void> completableFuture3 = completableFuture2.thenAcceptAsync(System.out::println);
        CompletableFuture<Void> completableFuture4 = completableFuture3.thenRunAsync(() -> System.out.println("end"));

        try {
            cyclicBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }

        System.out.println("main end");
    }

    @Test
    public void testWithFuture() {
        // 在一些情况下，获取到Future的代码我们改不了了，我们只能利用Future，那我们怎么结合Future和CompletableFuture来完成后续任务的安排和执行呢？
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        // 假设这块的代码我们不能改，我们只能获取到一个Future对象
        Future<String> future = executorService.submit(() -> {
            Thread.sleep(5000);
            return "hello world";
        });

        CountDownLatch countDownLatch = new CountDownLatch(1);
        // 使用CompletableFuture.supplyAsync方法，把获取Future的结果作为一个异步任务提交出去
        CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync(() -> {
            try {
                // 从Future中获取数据，然后把获取到的数据交给CompletableFuture，这样它就自动进行后续任务的处理了，不需要用户在手动指出后续任务了
                return future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        });

        // 此后主线程只需要为completableFuture布置后续任务即可
        completableFuture.thenApply(str -> str.split(" "))
                .thenApply(strArray -> strArray[1])
                .thenAccept(str -> System.out.println("end:" + str))
                .whenComplete((unused, throwable) -> countDownLatch.countDown());

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("main end");
    }

    @Test
    public void exercise() {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        // 此时completableFuture有可能还没有执行完成
        CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "hello world";
        });
        // 我们不需要关心completableFuture是否完成，只需要给completableFuture安排后续任务即可，由CompletableFuture自己去决定何时执行后续任务
        CompletableFuture<String[]> completableFuture1 = completableFuture.thenApplyAsync(str -> str.split(" "));
        CompletableFuture<String[]> completableFuture2 = completableFuture1.whenComplete((strArray, throwable) -> System.out.println(String.join(",", strArray)));
        CompletableFuture<Void> completableFuture3 = completableFuture2.thenAcceptAsync(strArray -> System.out.println(strArray[1]));
        CompletableFuture<Void> completableFuture4 = completableFuture3.thenRun(() -> {
            System.out.println("end");
            countDownLatch.countDown();
        });

        try {
            // 等待后续任务完成后，主线程再执行
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("main end");
    }
}
