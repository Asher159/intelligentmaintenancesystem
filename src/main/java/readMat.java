import java.sql.*;
import java.util.Arrays;

public class readMat {

    private static final String Class_Name = "org.sqlite.JDBC";
    private static final String DB_URL = "jdbc:sqlite:I:\\后期\\训练模型\\模型训练文件\\MT2_ae_raw\\原始数据db\\ae_MT2_DATA\\" +
            "20190423085442_inner_0.6_0.04_al7075_15000rpm_depth0.1_width3_feed2500\\data.db";

    public static void main(String[] args) {
        Connection connection = null;
        try {
            System.out.println(1);
            connection = createConnection();
            System.out.println(2);
            func1(connection);
            System.out.println("Success!");
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                // connection close failed.
                System.err.println(e);
            }
        }
    }

    // 创建Sqlite数据库连接
    public static Connection createConnection() throws SQLException, ClassNotFoundException {
        Class.forName(Class_Name);
        return DriverManager.getConnection(DB_URL);
    }

    public static void func1(Connection connection) throws SQLException {

        PreparedStatement preparedStatement = connection.prepareStatement("select data,data_len FROM sensor_data limit ?");
        preparedStatement.setInt(1, 20);

        //statement1.executeUpdate(" CREATE TABLE user_name( id integer,  name text )  ");

        // 执行插入语句操作
        //statement1.executeUpdate("insert into user_name(id, name) values('1','2')");
        // 执行更新语句
        //statement1.executeUpdate("update user_name set name= '222' where id ='1'");

        // 执行查询语句

        ResultSet rs = preparedStatement.executeQuery();
        int i = 1;
        while (rs.next()) {
            int data_len = rs.getInt("data_len");
            byte[] data = rs.getBytes("data");
            
            System.out.println(i++);

            System.out.println("id = " + data_len+ "  name = " + Arrays.toString(data));
        }
    }
}