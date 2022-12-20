package happydb;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

public class Main implements Runnable {
    public static int N = 160;
    public static int flag = 0;
    public static final ReentrantLock lock = new ReentrantLock();
    public static CountDownLatch latch = new CountDownLatch(N);
    public static List<Thread> threads = new ArrayList<>();
    @Override
    public void run() {
        try {
            latch.countDown();
            latch.await();

            lock.lock();
            N--;
            lock.unlock();

            if (N == 0) {
                flag++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i < N; i++) {
            threads.add(new Thread(new Main()));
            threads.get(threads.size() - 1).start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        System.out.println("flag = " + flag);
    }
}


