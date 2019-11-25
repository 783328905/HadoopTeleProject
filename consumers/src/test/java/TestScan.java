import com.ctillnow.constant.Constant;
import com.ctillnow.utils.HBaseFilterUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/24 23:41
 * 4
 */
public class TestScan {

    @Test
    public void scan() throws IOException {
        Filter filter1 = HBaseFilterUtil.eqFilter("f1", "call1", Bytes.toBytes("15369468720"));
        Filter filter2 = HBaseFilterUtil.eqFilter("f1", "call2", Bytes.toBytes("15369468720"));

        Filter filter3 = HBaseFilterUtil.orFilter(filter1, filter2);

        Filter filter4 = HBaseFilterUtil.gteqFilter("f1", "buildTime", Bytes.toBytes("2017-04"));
        Filter filter5 = HBaseFilterUtil.lteqFilter("f1", "buildTime", Bytes.toBytes("2017-07"));

        Filter filter6 = HBaseFilterUtil.andFilter(filter4, filter5);

        Filter filter7 = HBaseFilterUtil.andFilter(filter3, filter6);

        Scan scan = new Scan();
        scan.setFilter(filter7);

        Connection connection = ConnectionFactory.createConnection(Constant.CONF);
        Table table = connection.getTable(TableName.valueOf("ns_telecom:calllog"));

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell))
                        + "CN:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "VALUE:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

    }

}
