package com.iflytek.edu.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HbaseTest {

    /**
     * 配置ss
     */
    static Configuration config = null;
    private Connection connection = null;
    private Table table = null;
    private Admin admin = null;

    @Before
    public void init() throws Exception {
        config = HBaseConfiguration.create();// 配置都封装成<k,v>
        config.set("hbase.zookeeper.quorum", "192.168.56.101");// zookeeper地址
        config.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
        connection = ConnectionFactory.createConnection(config);
		/*connection.getTable(TableName.valueOf("test"))这种方式获得的连接是一种连接池的方式，
		也可以使用new HTable()的方式创建一个单连接，
		明显用连接池可以控制多个线程同时连接hbase的情况，优于new HTable()的方式创建一个单连接
		*/
        table = connection.getTable(TableName.valueOf("test2"));
        admin = connection.getAdmin();

    }

    /**
     * 创建一个表
     * 类似于shell命令中的create 'test3','info1','info2'  --创建test表和info1族与info2族
     * @throws Exception
     */
    @Test
    public void createTable() throws Exception {
        // 创建表描述类
        TableName tableName = TableName.valueOf("test2"); // 表名称
        HTableDescriptor desc = new HTableDescriptor(tableName);
        // 创建列族的描述类
        HColumnDescriptor family = new HColumnDescriptor("info"); // 列族
        // 将列族添加到表中
        desc.addFamily(family);
        HColumnDescriptor family2 = new HColumnDescriptor("info2"); // 列族
        // 将列族添加到表中
        desc.addFamily(family2);
        // 创建表
        admin.createTable(desc); // 创建表
    }

    /*
    删除表test2
    * */
    @Test
    @SuppressWarnings("deprecation")
    public void deleteTable() throws MasterNotRunningException,
            ZooKeeperConnectionException, Exception {
        HBaseAdmin admin = new HBaseAdmin(config);
        //删除表之前必须将表disabled
        admin.disableTable("test2");
        admin.deleteTable("test2");
        admin.close();
    }

    /**
     * 向hbase中增加数据
     *
     * @throws Exception
     */
    @SuppressWarnings({ "deprecation", "resource" })
    @Test
    public void insertData() throws Exception {
        Put put = new Put(Bytes.toBytes("11"));
        put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("zhangsan"));
        //插入多个put时可以new List<put>,然后table.put(list);
        //插入的数据是字典序排序的，后面添加的数据会覆盖原来的数据
        table.put(put);
    }

    /**
     * 修改数据
     *
     * @throws Exception
     */
    @Test
    public void updateData() throws Exception {
        Put put = new Put(Bytes.toBytes("10"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("lisi1234"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(1234));
        //插入数据
        table.put(put);
    }

    /**
     * 删除数据
     *
     * @throws Exception
     */
    @Test
    public void deleteDate() throws Exception {
        //删除某个id的一行数据
		/*Delete delete = new Delete(Bytes.toBytes("1234"));
		table.delete(delete);
		table.flushCommits();*/

        //删除某个列族，或者删除某个列
        Delete delete = new Delete(Bytes.toBytes("1234"));
        //删除列族
        delete.addFamily(Bytes.toBytes("info"));
        //删除列
        delete.addColumn(Bytes.toBytes("info2"),Bytes.toBytes("age"));
        table.delete(delete);
    }

    /**
     * 单条查询
     *
     * @throws Exception
     */
    @Test
    public void queryData() throws Exception {
        //查询一行行键为10的数据
        Get get = new Get(Bytes.toBytes("10"));
        Result result = table.get(get);
        //result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"))获取info1:name这一列的数据
        System.out.println("info:name为"+Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
        System.out.println("info2:age为"+Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age"))));

        System.out.println("-------------------------分隔符--------------------------");

        //只查询某一行
        Get get2 = new Get(Bytes.toBytes("10"));

        get2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

        Result result2 = table.get(get2);
        //result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))info:name这一列的数据
        System.out.println("info:name为"+Bytes.toString(result2.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
        System.out.println("info2:age为"+Bytes.toString(result2.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age"))));

    }

    /**
     * 全表扫描
     *
     * @throws Exception
     */
    @Test
    public void scanData() throws Exception {
        Scan scan = new Scan();
        //下面两行代码表示：从主键为10扫描到11就停止了
        scan.setStartRow(Bytes.toBytes("10"));
        scan.setStopRow(Bytes.toBytes("12"));

        //全表扫描某一个列：此时除了info1：name列，其他列都为空
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age"))));
            System.out.println();
        }
    }

    /**
     * 全表扫描的过滤器
     * 列值过滤器:用来定义“列”的<列名：列值>过滤规则：如过滤列info1:name=zhangsan1的列，
     * 凡是info1:name=zhangsan1的列所在的行都过滤掉出来
     * @throws Exception
     */
    @Test
    public void scanDataByFilter1() throws Exception {

        // 创建全表扫描的scan
        Scan scan = new Scan();
        //过滤器：列值过滤器:参数1：过滤的列族  参数2：过滤的列名  参数3：过滤的规则（大于等于小于）  参数4：与参数3比较的值
        //CompareFilter.CompareOp.EQUAL表示行健等于10
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"),
                Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("zhangsan"));
        // 设置过滤器
        scan.setFilter(filter);

        // 打印结果集
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println("id" + Bytes.toString(result.getRow()));
            System.out.println("info:name:" + Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
            System.out.println("info2:age" + Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age"))));
            System.out.println();
        }

    }


    /**
     * rowkey过滤器:列名过滤器，过滤主键(正则表达式)
     * @throws Exception
     */
    @Test
    public void scanDataByFilter2() throws Exception {

        // 创建全表扫描的scan
        Scan scan = new Scan();
        //匹配rowkey以wangsenfeng开头的
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("[^1]"));
        // 设置过滤器
        scan.setFilter(filter);
        // 打印结果集
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println("id= " + Bytes.toString(result.getRow()));
            System.out.println("info:name=" + Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
            System.out.println("info2:age=" + Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age"))));
            System.out.println();
        }


    }

    /**
     * 匹配列名前缀
     * @throws Exception
     */
    @Test
    public void scanDataByFilter3() throws Exception {

        // 创建全表扫描的scan
        Scan scan = new Scan();
        //匹配列名中含有以na开头的列（如：含有info1：name的行）所在的行
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("na"));
        // 设置过滤器
        scan.setFilter(columnPrefixFilter);
        // 打印结果集
        ResultScanner results = table.getScanner(scan);

        for (Result result:results){
            System.out.println("id=" + Bytes.toString(result.getRow()));
            System.out.println("info:name=" + Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("name"))));
            System.out.println("info2:age=" + Bytes.toString(result.getValue(Bytes.toBytes("info2"),Bytes.toBytes("age"))));
            System.out.println();
        }
    }

    /**
     * 匹配列名多个前缀
     * @throws Exception
     */
    @Test
    public void scanDataByFilter4() throws Exception {

        // 创建全表扫描的scan
        Scan scan = new Scan();
        //匹配列名中含有以nam开头的列和ag（如：含有info1：name的行和info2：age的行）所在的行
        byte[][] bytes = new byte[][]{Bytes.toBytes("nam"),Bytes.toBytes("ag")};
        MultipleColumnPrefixFilter mcpfilter = new MultipleColumnPrefixFilter(bytes);
        // 设置过滤器
        scan.setFilter(mcpfilter);
        // 打印结果集
        ResultScanner results = table.getScanner(scan);

        for (Result result:results){
            System.out.println("id=" + Bytes.toString(result.getRow()));
            System.out.println("info:name=" + Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("name"))));
            System.out.println("info2:age=" + Bytes.toString(result.getValue(Bytes.toBytes("info2"),Bytes.toBytes("age"))));
            System.out.println();
        }
    }

    /**
     * 过滤器集合:同时使用多个过滤器，有两种方式：多个过滤器是MUST_PASS_ALL（and）,MUST_PASS_ONE(or)
     * @throws Exception
     */
    @Test
    public void scanDataByFilter5() throws Exception {

        // 创建全表扫描的scan
        Scan scan = new Scan();
        //过滤器集合：MUST_PASS_ALL（and）,MUST_PASS_ONE(or)
        FilterList filterList = new FilterList(Operator.MUST_PASS_ONE);
        //匹配rowkey以wangsenfeng开头的
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^10"));//以20开头的
        //匹配name的值等于wangsenfeng
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"),
                Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("zhangsan"));//“info1：name = zhangsan”的行
        filterList.addFilter(filter);
        filterList.addFilter(filter2);
        // 设置过滤器
        scan.setFilter(filterList);
        // 打印结果集
        ResultScanner scanner = table.getScanner(scan);

        for (Result result:scanner) {
            System.out.println("id=" + Bytes.toString(result.getRow()));
            System.out.println("info:name=" + Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("name"))));
            System.out.println("info2:age=" + Bytes.toString(result.getValue(Bytes.toBytes("info2"),Bytes.toBytes("age"))));
            System.out.println();
        }
    }

    @After
    public void close() throws Exception {
        table.close();
        connection.close();
    }

}
