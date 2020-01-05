import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

/**
 * zkClient based on curator.
 * <p>
 * Reference:
 * <p>
 * <a href="https://curator.apache.org/">apache-curator-official-website</a>
 * <p>
 * <a href="https://github.com/apache/curator/blob/master/curator-examples/src/main/java/framework/CrudExamples.java">CrudExamples</a>
 *
 * @author sq.ma
 * @date 2020/1/3 上午9:35
 */
public class CuratorZkClient {

    /**
     * Build Zookeeper framework-style client
     * Use {@link ExponentialBackoffRetry} as retryPolicy
     */
    private static final CuratorFramework CURATOR_FRAMEWORK = CuratorFrameworkFactory.builder()
            .connectString("192.168.1.220:2181")
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build();

    private static final String PATH = "/option";

    private static final byte[] DATA = "test".getBytes();

    private static final byte[] DATA_UPDATED = "test_update".getBytes();


    public static void main(String[] args) throws Exception {
        CURATOR_FRAMEWORK.start();
        /**
         * CURD demo
         */
        //create
        createNode(CURATOR_FRAMEWORK, PATH, DATA);
        //update
        updateNode(CURATOR_FRAMEWORK, PATH, DATA_UPDATED);
        //retrieve
        retrieveNode(CURATOR_FRAMEWORK, PATH);
        //delete
        deleteNode(CURATOR_FRAMEWORK, PATH);
        CURATOR_FRAMEWORK.close();
    }

    /**
     * create an persistent node
     */
    private static void createNode(CuratorFramework curatorFramework, String path, byte[] data) throws Exception {
        //check existence of node
        Stat stat = curatorFramework.checkExists().forPath(path);
        if (stat == null) {
            curatorFramework.create()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(ZooDefs.Ids.CREATOR_ALL_ACL)
                    .forPath(path, data);
            System.out.println("Node Created: " + new String(data));
        } else {
            System.out.println("Node does exist");
        }

    }

    /**
     * retrieve nodes
     */
    private static void retrieveNode(CuratorFramework curatorFramework, String path) throws Exception {
        //check existence of node
        Stat stat = curatorFramework.checkExists().forPath(path);
        if (stat != null) {
            String data = new String(curatorFramework.getData().forPath(path));
            System.out.println("Node Fined: " + data);
        } else {
            System.out.println("Node does not exist");
        }
    }

    /**
     * update nodes
     */
    private static void updateNode(CuratorFramework curatorFramework, String path, byte[] data) throws Exception {
        //check existence of node
        Stat stat = curatorFramework.checkExists().forPath(path);
        if (stat != null) {
            curatorFramework.setData().forPath(path, data);
            System.out.println("Node Updated: " + new String(data));
        } else {
            System.out.println("Node does not exist");
        }
    }

    /**
     * delete nodes
     */
    private static void deleteNode(CuratorFramework curatorFramework, String path) throws Exception {
        //check existence of node
        Stat stat = curatorFramework.checkExists().forPath(path);
        if (stat != null) {
            curatorFramework.delete().forPath(PATH);
            System.out.println("Node Deleted");
        } else {
            System.out.println("Node does not exist");
        }
    }


}
