package com.ctillnow.com.ctillnow.OutPutFormat;

import com.ctillnow.com.ctillnow.kv.CommonDimension;
import com.ctillnow.com.ctillnow.kv.CountDurationValue;
import com.ctillnow.com.ctillnow.util.DimensionConverter;
import com.ctillnow.com.ctillnow.util.DimensionConverterImpl;

import com.ctillnow.com.ctillnow.util.JDBCUtil;
import org.apache.hadoop.fs.Path;


import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/31 14:51
 * 4
 */
public class MysqlOutPutFormat extends OutputFormat<CommonDimension,CountDurationValue>{
    private OutputCommitter committer = null;


    @Override
    public RecordWriter<CommonDimension, CountDurationValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Connection connection = JDBCUtil.getInstance();
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }


        return new MysqlDBRecoderWriter(connection);

    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }
    public static class MysqlDBRecoderWriter extends RecordWriter<CommonDimension,CountDurationValue>{
        private  Connection connection;
        private PreparedStatement preparedStatement= null;
        private int batchBound = 500;
        private int batchSize =0;
        private DimensionConverter convertor= null;


        public MysqlDBRecoderWriter(Connection connection){
            this.connection = connection;
            this.convertor = new DimensionConverterImpl();

        }

        @Override
        public void write(CommonDimension commonDimension, CountDurationValue countDurationValue) throws IOException, InterruptedException {

            String sql = "INSERT INTO tb_call VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `call_sum` = ?,`call_duration_sum` = ?;";

            try {
                int contactId = convertor.getDimensionID(commonDimension.getContactDimension());
                int dateId = convertor.getDimensionID(commonDimension.getDateDimension());

                //2.获取通话次数&通话时长
                int count = countDurationValue.getCount();
                int duration = countDurationValue.getDuration();
                //拼接tb_call表的主键
                String callKey = contactId + "_" + dateId;

                //初始化预编译的sql
                if (preparedStatement == null) {
                    preparedStatement = connection.prepareStatement(sql);
                }

                //给预编译sql赋值
                int i = 0;
                preparedStatement.setString(++i, callKey);
                preparedStatement.setInt(++i, dateId);
                preparedStatement.setInt(++i, contactId);
                preparedStatement.setInt(++i, count);
                preparedStatement.setInt(++i, duration);
                preparedStatement.setInt(++i, count);
                preparedStatement.setInt(++i, duration);

                //将数据放入缓存
                preparedStatement.addBatch();
                batchSize++;

                //当缓存数据条数超过预设边界时，执行sql并提交
                if (batchSize >= batchBound) {
                    preparedStatement.executeBatch();
                    connection.commit();
                    batchSize = 0;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (preparedStatement != null) {
                try {
                    preparedStatement.executeBatch();
                    connection.commit();

                    JDBCUtil.close(connection, preparedStatement, null);
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        }
    }
}

