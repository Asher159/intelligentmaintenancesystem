import java.io.IOException;
import java.text.SimpleDateFormat;
//import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Date;
//import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
//import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.poi.ss.usermodel.Row;
//import org.apache.poi.ss.usermodel.Sheet;

/**
 * version hbase 1.1.2 hbase操作 创建表 1.通过HBaseConfiguration.create() ：获取配置 conf
 * 2.conf.set() ：设置zk等参数（kerberos认证等）
 * 3.ConnectionFactory.createConnection(configuration) ：获取连接conn
 * 4.通过conn.getAdmin()来获取Admin ：表相关操作的类 (HBaseAdmin已过期) 5.创建TableName：描述表名称的 ：
 * TableName tname = TableName.valueOf(tablename); 6.创建表描述信息类: HTableDescriptor
 * tDescriptor = new HTableDescriptor(tname); 7.添加表列簇描述信息类：HColumnDescriptor
 * famliy = new HColumnDescriptor(cf);
 * 8.将表列簇描述信息类添加到表描述信息类：tDescriptor.addFamily(famliy);
 * 9.调用admin创建表：admin.createTable(tDescriptor);
 *
 * hbase操作 添加数据 1.通过HBaseConfiguration.create() ：获取配置 conf 2.conf.set()
 * ：设置zk等参数（kerberos认证等） 3.ConnectionFactory.createConnection(configuration)
 * ：获取连接conn 4.创建TableName：描述表名称的 ： TableName tname =
 * TableName.valueOf(tablename); 5.通过conn连接获得表 对象 ：Table table =
 * connection.getTable(tableName); 6.1.单挑插入table.put(Put)
 * 6.2.批量插入数据，先用list封装put对象：List<Put> batPut = new ArrayList<Put>(); Put put =
 * new Put(Bytes.toBytes("rowkey_"+i)); //插入的rowkey
 * put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("username"),
 * Bytes.toBytes("un_"+i)); //列簇，列，值 batPut.add(put) table.put(batPut)
 *
 *
 * hbase操作 获取数据 1.通过HBaseConfiguration.create() ：获取配置 conf 2.conf.set()
 * ：设置zk等参数（kerberos认证等） 3.ConnectionFactory.createConnection(configuration)
 * ：获取连接conn 4.创建TableName：描述表名称的 ： TableName tname =
 * TableName.valueOf(tablename); 5.通过conn连接获得表 对象 ：Table table =
 * connection.getTable(tableName); 6.List<Get> gets = new ArrayList<Get>();
 * //批量封装请求信息 Get get = new Get(Bytes.toBytes("rowkey_"+i)); //查询的rowkey
 * gets.add(get); 7.Result[] results = table.get(gets); //通过Result[]接收数据
 * 8.使用CellScanner cellScanner = result.cellScanner(); 获取cell
 * while(cellScanner.advance()){ Cell cell = cellScanner.current();
 * //从单元格cell中把数据获取并输出 //使用 CellUtil工具类，从cell中把数据获取出来 String famliy =
 * Bytes.toString(CellUtil.cloneFamily(cell)); String qualify =
 * Bytes.toString(CellUtil.cloneQualifier(cell)); String rowkey =
 * Bytes.toString(CellUtil.cloneRow(cell)); String value =
 * Bytes.toString(CellUtil.cloneValue(cell));
 * System.out.println("rowkey:"+rowkey+",columnfamily:"+famliy+",qualify:"+qualify+",value:"+value);
 * }
 *
 * @author asher
 *
 */
public class hbaseopt {
    public Connection connection; // 用hbaseconfiguration初始化配置信息时会自动加载当前应用classpath下的hbase-site.xml
    public static Configuration configuration = HBaseConfiguration.create();
    public Table table;
    public Admin admin;
    public HBaseAdmin ad;

    public hbaseopt() throws Exception {
        // ad = new HBaseAdmin(configuration); //过期了，推荐使用Admin

        configuration.set("hbase.zookeeper.quorum",
                "192.168.1.211:2181,192.168.1.212:2181,192.168.1.213:2181,192.168.1.214:2181");
//	        configuration.set("hbase.zookeeper.property.clientPort","2181");
        configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
        // 对connection初始化
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

    // 创建表
    public void createTable(String tablename, String... cf1) throws Exception {
        // 获取admin对象
        Admin admin = connection.getAdmin();
        // 创建tablename对象描述表的名称信息
        TableName tname = TableName.valueOf(tablename);// bd17:mytable
        // 创建HTableDescriptor对象，描述表信息
        HTableDescriptor tDescriptor = new HTableDescriptor(tname);
        // 判断是否表已存在
        if (admin.tableExists(tname)) {
            System.out.println("表" + tablename + "已存在");
            return;
        } else {
            // 添加表列簇信息
            for (String cf : cf1) {
                HColumnDescriptor famliy = new HColumnDescriptor(cf);
                tDescriptor.addFamily(famliy);
            }
            // 调用admin的createtable方法创建表
            admin.createTable(tDescriptor);
            System.out.println("表" + tablename + "创建成功");
        }
    }

    // 删除表
    public void deleteTable(String tablename) throws Exception {
        Admin admin = connection.getAdmin();
        TableName tName = TableName.valueOf(tablename);
        if (admin.tableExists(tName)) {
            admin.disableTable(tName);
            admin.deleteTable(tName);
            System.out.println("删除表" + tablename + "成功！");
        } else {
            System.out.println("表" + tablename + "不存在。");
        }
    }

    // 新增数据到表里面Put
//    public void putData(String table_name, Sheet sheet) throws Exception {
//        TableName tableName = TableName.valueOf(table_name);
//        Table table = connection.getTable(tableName);
////		int size = sheet.getLastRowNum();
//        String family = sheet.getSheetName();
////		Integer rowsums = sheet.getLastRowNum();
//        List<Put> batPut = new ArrayList<Put>();
//        List<String> columnName = new ArrayList<String>();
//
//        for (Row row : sheet) {
//            if (row.getRowNum() == 0) {
//                for (org.apache.poi.ss.usermodel.Cell cell : row) {
//                    columnName.add(cell.getStringCellValue());
//                }
//                System.out.println(columnName);
//            }
//            // 遍历所有的列
//            int i = 0;
//            if (row.getRowNum() > 0) {
//                for (org.apache.poi.ss.usermodel.Cell cell : row) {
//                    Put put = new Put(Bytes.toBytes(row.getRowNum())); // 行键
//                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName.get(i)),
//                            Bytes.toBytes(cell.getStringCellValue())); // 列族，列名，值
//                    i++;
//                    // 单记录put
////	            table.put(put);
//                    batPut.add(put);
//                }
//            }
//            if (batPut.size() > 200000) { // 设置的缓冲区。一个table.put代表一个RPC，如果多次put将造成插入异常缓慢，所以得设置缓冲区
//                table.put(batPut);
//                batPut.clear();
//            }
//        }
//        table.put(batPut);
//        batPut.clear();
//        System.out.println("表插入数据成功！");
//    }

    public List<String> getDataOriginal(String table_Name, int Num0, int Num1) throws Exception {
        List<String> list1 = new ArrayList<>();
        TableName tableName = TableName.valueOf(table_Name);
        table = connection.getTable(tableName);

        // 构建get对象
        List<Get> gets = new ArrayList<Get>();
        for (int i = Num0; i < Num0 + Num1; i++) {
            Get get = new Get(Bytes.toBytes("rowkey_" + i));
            gets.add(get);
        }
        Num0 += Num1;
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String startTime = df.format(new Date());
        Result[] results = table.get(gets);
        for (Result result : results) {

            /*
             * 第一种获取值的方法 Cell[] cells = result.rawCells(); for (Cell cell : cells) {
             * System.out.println( Bytes.toString(cell.getRowArray(), cell.getRowOffset(),
             * cell.getRowLength()) +"  "+ Bytes.toString(cell.getFamilyArray(),
             * cell.getFamilyOffset(), cell.getFamilyLength()) +"  "+
             * Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
             * cell.getQualifierLength()) +"  "+ Bytes.toString(cell.getValueArray(),
             * cell.getValueOffset(), cell.getValueLength())); }
             **/
//			table.close();

            CellScanner cellScanner = result.cellScanner();
            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();
                // 从单元格cell中把数据获取并输出
                // 使用 CellUtil工具类，从cell中把数据获取出来
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                String famliy = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualify = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(rowkey + famliy + qualify + value);
                list1.add(value);
            }
        }
        String endTime = df.format(new Date());
        System.out.println(startTime + "  " + endTime);
//		Iterator<String> it1 = list1.iterator();
        return list1;
    }

    public List<String> getData(String table_Name, int Num0, int Num1) throws Exception {
        List<String> list1 = new ArrayList<>();
        TableName tableName = TableName.valueOf(table_Name);
        table = connection.getTable(tableName);

        FilterList list = new FilterList(Operator.MUST_PASS_ALL);
        Scan scan = new Scan();
        RowFilter filter = new RowFilter(CompareOp.GREATER_OR_EQUAL,new BinaryComparator(Bytes.toBytes(Num0)));
        list.addFilter(filter);
        RowFilter filter2 = new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(Num1)));
        list.addFilter(filter2);
        scan.setFilter(list);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            CellScanner cellScanner = rs.cellScanner();
            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();
                // 从单元格cell中把数据获取并输出
                // 使用 CellUtil工具类，从cell中把数据获取出来
//				int rowkey = Bytes.toInt(CellUtil.cloneRow(cell));
//				String famliy = Bytes.toString(CellUtil.cloneFamily(cell));
//				String qualify = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
//				System.out.println(rowkey + famliy + qualify + value);
                list1.add(value);
            }
        }
        return list1;
    }

    public void deleteData() throws IOException {
        Table table = connection.getTable(TableName.valueOf("test_web"));
        // 参数为 row key
        // 删除一列
        Delete delete1 = new Delete(Bytes.toBytes("rowkey1"));
        delete1.addColumn(Bytes.toBytes("family1"), Bytes.toBytes("name1"));
        // 删除多列
        Delete delete2 = new Delete(Bytes.toBytes("rowkey2"));
        delete2.addColumns(Bytes.toBytes("family1"), Bytes.toBytes("age"));
        delete2.addColumns(Bytes.toBytes("family1"), Bytes.toBytes("sex"));
        // 删除某一行的列族内容
        Delete delete3 = new Delete(Bytes.toBytes("rowkey3"));
        delete3.addFamily(Bytes.toBytes("family1"));

        // 删除一整行
        Delete delete4 = new Delete(Bytes.toBytes("rowkey2"));
        table.delete(delete1);
        table.delete(delete2);
        table.delete(delete3);
        table.delete(delete4);
        table.close();
    }

    public void singColumnFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf("test_web"));
        Scan scan = new Scan();
        // 当该行中的对应值相等即被选中，但是当该行中没有所选的项目值时亦会被选中
        // 下列参数分别为，列族，列名，比较符号，值
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("family2"), Bytes.toBytes("name1"),
                CompareOp.EQUAL, Bytes.toBytes("cm"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(
                        Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                                + "::"
                                + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    public void rowkeyFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf("test_web"));
        Scan scan = new Scan();
        RowFilter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("rowkey2"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(
                        Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                                + "::"
                                + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    // 关闭连接
    public void cleanUp() throws Exception {
        connection.close();
    }

//    public static void main(String[] args) throws Exception {
////		writeToHbase hbaseTest = new writeToHbase();
////		List<String> resultIterator = hbaseTest.getData2("CS1_53424", 1, 10);
////		System.out.println(resultIterator);
////		hbaseTest.cleanUp();
//
//        writeToHbase hbaseTest = new writeToHbase();
////		hbaseTest.createTable("fre_CS1_53424", "Spindle","Current");
//        readExcelStream a = new readExcelStream("C:\\Users\\foxconn\\Desktop\\json_test\\CS1_53424.xlsx", 2);
//        Sheet sheet = a.testLoad();
//        hbaseTest.putData("fre_CS1_53424",sheet);
//
////		writeToHbase hbaseTest = new writeToHbase();
////		List<String> resultIterator = hbaseTest.getData("fre_CS1_53424", 1,3000);
////		System.out.println(resultIterator);
////		hbaseTest.cleanUp();
//    }
}