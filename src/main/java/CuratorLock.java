import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Curator lock demo
 *
 * @author sq.ma
 * @date 2020/1/22 下午5:32
 */
public class CuratorLock {

    private static final CuratorFramework CURATOR_FRAMEWORK = CuratorFrameworkFactory.builder()
            .connectString("192.168.0.220:2181")
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build();


    public static void main(String[] args) {
        CURATOR_FRAMEWORK.start();
        final InterProcessMutex lock = new InterProcessMutex(CURATOR_FRAMEWORK, "/locks");

        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                try {
                    System.err.println(Thread.currentThread().getName() + "try to acquire lock");
                    lock.acquire();
                    System.err.println(Thread.currentThread().getName() + "has acquired lock");
                    Thread.sleep(4000);
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    try {
                        lock.release();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, "thread-" + i).start();
        }
    }
}
