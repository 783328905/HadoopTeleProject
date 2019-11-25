import com.ctillnow.constant.Constant;
import com.ctillnow.utils.HBaseFilterUtil;
import com.ctillnow.utils.HBaseScanFilter;
import com.ctillnow.utils.HBaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/30 16:43
 * 4
 */
public class TestScan2 {

    public static void main(String[] args) throws ParseException, IOException {
        List<String[]> startStop = HBaseScanFilter.getStartStop("13256116204","2018-03","2018-06");

        Connection connection = ConnectionFactory.createConnection(Constant.CONF);
        Table table = connection.getTable(TableName.valueOf("ns_telecom:calllog"));

        while (HBaseScanFilter.hasNext()) {

            String[] next = HBaseScanFilter.next();
            System.out.println(next[0] + next[1]);
            Scan scan = new Scan(Bytes.toBytes(next[0]), Bytes.toBytes(next[1]));
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                System.out.println(Bytes.toString(result.getRow()));
            }

        }






    }

    @Test
    public void test1(){
        List list = new ArrayList();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        System.out.println( list.get(0));
        System.out.println( list.get(1));

    }
}
