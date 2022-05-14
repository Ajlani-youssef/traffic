import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class GetTraffic {

    private Table table1;
    private String tableName = "current_traffic";

    public void readHBaseData() throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        table1 = connection.getTable(TableName.valueOf(tableName));

        try {
            Scan scan = new Scan();

            ResultScanner scanner = table1.getScanner(scan);
            for (Result result : scanner) {
                byte[] values = result.getValue(Bytes.toBytes("route"),Bytes.toBytes("speed"));
                String key = new String(result.getRow());
                String s  =  new  String(values);
                System.out.println("route : " +  key + "  , vitesse : " + s);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            table1.close();
            connection.close();
        }
    }

    public static void main(String[] args) throws IOException {
        GetTraffic admin = new GetTraffic();
        admin.readHBaseData();
    }
}
