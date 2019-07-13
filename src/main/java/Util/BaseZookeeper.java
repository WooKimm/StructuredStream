package Util;

import java.io.*;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;



public class BaseZookeeper implements Watcher{
    
        private ZooKeeper zookeeper;
        /**
      * 超时时间
      */
        private static final int SESSION_TIME_OUT = 2000;
        private CountDownLatch countDownLatch = new CountDownLatch(1);
        @Override
        public void process(WatchedEvent event) {
        if (event.getState() == KeeperState.SyncConnected) {
            System.out.println("Watch received event");
                 countDownLatch.countDown();
         }
         }



/**连接zookeeper
     * @param host
     * @throws Exception
     */
         public void connectZookeeper(String host) throws Exception{
             zookeeper = new ZooKeeper(host, SESSION_TIME_OUT, this);
             countDownLatch.await();
            System.out.println("zookeeper connection success");
         }
/**
     * 创建节点
     * @param path
     * @param data
     * @throws Exception
     */
        public String createNode(String path,String data) throws Exception{
            return this.zookeeper.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
 /**
     * 获取路径下所有子节点
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
        public List<String> getChildren(String path) throws KeeperException, InterruptedException{
            List<String> children = zookeeper.getChildren(path, false);
            return children;
        }
        /**
     * 获取节点上面的数据
     * @param path  路径
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
        public String getData(String path) throws KeeperException, InterruptedException{
            byte[] data = zookeeper.getData(path, false, null);
            if (data == null) {
                return "";
            }
            return new String(data);
         }
        /**
     * 设置节点信息
     * @param path  路径
     * @param data  数据
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
         public Stat setData(String path,String data) throws KeeperException, InterruptedException{
            Stat stat = zookeeper.setData(path, data.getBytes(), -1);
            return stat;
        }
        /**
     * 删除节点
     * @param path
     * @throws InterruptedException
     * @throws KeeperException
     */
        public void deleteNode(String path) throws InterruptedException, KeeperException{
            zookeeper.delete(path, -1);
        }
        /**
     * 获取创建时间
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
        public String getCTime(String path) throws KeeperException, InterruptedException{
            Stat stat = zookeeper.exists(path, false);
            return String.valueOf(stat.getCtime());
        }
        /**
     * 获取某个路径下孩子的数量
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
        public Integer getChildrenNum(String path) throws KeeperException, InterruptedException{
            int childenNum = zookeeper.getChildren(path, false).size();
            return childenNum;
        }
 /**
     * 关闭连接
     * @throws InterruptedException
     */
        public void closeConnection() throws InterruptedException{
            if (zookeeper != null) {
                zookeeper.close();
            }

        }

    public String readToString(String fileName) {
        String encoding = "UTF-8";
        File file = new File(fileName);
        Long filelength = file.length();
        byte[] filecontent = new byte[filelength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(filecontent);
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            return new String(filecontent, encoding);
        } catch (UnsupportedEncodingException e) {
            System.err.println("The OS does not support " + encoding);
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) throws Exception{
        BaseZookeeper zookeeper = new BaseZookeeper();
        zookeeper.connectZookeeper("127.0.0.1:2181");
//        String data = zookeeper.readToString("E:\\Woo\\2019summerPra\\spark1\\src\\main\\resources\\testJsonSQL");
//        String result = zookeeper.createNode("/jsonSQL",data);


//        Stat result = zookeeper.setData("/csvSQL","create env spark(\n" +
//                "    spark.default.parallelism='2',\n" +
//                "    spark.sql.shuffle.partitions='2'\n" +
//                ")WITH(\n" +
//                "    appname='CsvTest'\n" +
//                ");\n" +
//                "\n" +
//                "CREATE TABLE csvTable(\n" +
//                "    name string,\n" +
//                "    age int\n" +
//                ")WITH(\n" +
//                "    type='csv',\n" +
//                "    delimiter=';',\n" +
//                "    processwindow='10 seconds,10 seconds',\n" +
//                "    path='E:\\Woo\\2019summerPra\\spark1\\filepath'\n" +
//                ");\n" +
//                "\n" +
//                "create SINK console(\n" +
//                ")WITH(\n" +
//                "    type='console',\n" +
//                "    outputmode='update',\n" +
//                ");\n" +
//                "\n" +
//                "insert into console select * from csvTable;");


        //System.out.println(result);
//        List<String> children = zookeeper.getChildren("/");
//        for(String child:children){
//            System.out.println(child);
//        }
    }

    public static String getSqlFromSource(String type)
    {
        File file = null;
        switch (type)
        {
            case "csv":
                file = new File("src/main/resources/testCsvSQL");
                break;
            case "json":
                file = new File("src/main/resources/testJsonSQL");
                break;
            case "kafka":
                file = new File("src/main/resources/testKafkaSQL");
                break;
            case "socket":
                file = new File("src/main/resources/testSQLFile");
                break;
            case "orc":
                file = new File("src/main/resources/testOrcFile");
                break;
            case "parquet":
                file = new File("src/main/resources/testParquetSQL");
                break;
            case "rate":
                file = new File("src/main/resources/testRateSQL");
                break;
            case "kafkaOut":
                file = new File("src/main/resources/testKafkaOutput");
                break;
            case "csvOut":
                file = new File("src/main/resources/testCsvOutput");
                break;
            case "jsonOut":
                file = new File("src/main/resources/testJsonOutput");
                break;
            case "parquetOut":
                file = new File("src/main/resources/testParquetOutput");
                break;
            case "orcOut":
                file = new File("src/main/resources/testOrcOutput");
                break;
        }
        BufferedReader reader = null;
        String sql = "";
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                tempString = tempString + "\n";
                sql += tempString;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return sql;
    }

}
