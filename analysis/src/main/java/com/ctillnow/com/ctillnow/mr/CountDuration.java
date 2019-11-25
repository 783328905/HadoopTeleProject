package com.ctillnow.com.ctillnow.mr;

import com.ctillnow.com.ctillnow.OutPutFormat.MysqlOutPutFormat;
import com.ctillnow.com.ctillnow.kv.CommonDimension;
import com.ctillnow.com.ctillnow.kv.ContantDimension;
import com.ctillnow.com.ctillnow.kv.CountDurationValue;
import com.ctillnow.com.ctillnow.kv.DateDimension;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/30 19:09
 * 4
 */
public class CountDuration extends Configured  implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CountDuration(),args);
    }
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration,"count");

        job.setJarByClass(CountDuration.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("f1"));
        TableMapReduceUtil.initTableMapperJob("ns_telecom:calllog",scan,CountDurationMapper.class,CommonDimension.class,Text.class,job);

        job.setReducerClass(CountDurationReducer.class);
        job.setOutputFormatClass(MysqlOutPutFormat.class);


        job.waitForCompletion(true);


        return 0;
    }

    public static class CountDurationMapper extends TableMapper<CommonDimension,Text>{


        //公共维度
        private CommonDimension commonDimension = new CommonDimension();

        private Text v = new Text();
        private ContantDimension contantDimension = new ContantDimension();
        private DateDimension dateDimension = new DateDimension();
        private Map<String,String > contacts= null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            contacts = new HashMap();
            contacts.put("15369468720", "李雁");
            contacts.put("19920860202", "卫艺");
            contacts.put("18411925860", "仰莉");
            contacts.put("14473548449", "陶欣悦");
            contacts.put("18749966182", "施梅梅");
            contacts.put("19379884788", "金虹霖");
            contacts.put("19335715448", "魏明艳");
            contacts.put("18503558939", "华贞");
            contacts.put("13407209608", "华啟倩");
            contacts.put("15596505995", "仲采绿");
            contacts.put("17519874292", "卫丹");
            contacts.put("15178485516", "戚丽红");
            contacts.put("19877232369", "何翠柔");
            contacts.put("18706287692", "钱溶艳");
            contacts.put("18944239644", "钱琳");
            contacts.put("17325302007", "缪静欣");
            contacts.put("18839074540", "焦秋菊");
            contacts.put("19879419704", "吕访琴");
            contacts.put("16480981069", "沈丹");
            contacts.put("18674257265", "褚美丽");
            contacts.put("18302820904", "孙怡");
            contacts.put("15133295266", "许婵");
            contacts.put("17868457605", "曹红恋");
            contacts.put("15490732767", "吕柔");
            contacts.put("15064972307", "冯怜云");
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //05_15980356666_2018-06-14 02:03:30_1528913010000_13275681557_0_0999
            String row = Bytes.toString(key.get());
            String[] split = row.split("_");
            if("0".equals(split[5])){
                return;
            }
            String caller = split[1];
            String buildTime = split[2];
            String callee = split[4];
            String duration = split[6];

            String year = buildTime.substring(0,4);
            String month = buildTime.substring(5,7);
            String day = buildTime.substring(8,10);


            v.set(duration);

            contantDimension.setPhoneNum(caller);
            contantDimension.setName(contacts.get(caller));
            commonDimension.setContactDimension(contantDimension);


            //主叫联系人 年月日维度

            //年维度
            DateDimension yearDimension = new DateDimension(year,"-1","-1");
            commonDimension.setDateDimension(yearDimension);
            context.write(commonDimension,v);
            //月维度
            DateDimension monthDimension = new DateDimension(year, month, "-1");
            commonDimension.setDateDimension(monthDimension);
            context.write(commonDimension,v);

            //日维度
            DateDimension dayDimension = new DateDimension(year, month, day);
            commonDimension.setDateDimension(dayDimension);
            context.write(commonDimension,v);

            //被叫联系人 年月日维度

            contantDimension.setPhoneNum(callee);
            contantDimension.setName(contacts.get(callee));
            commonDimension.setContactDimension(contantDimension);

            commonDimension.setDateDimension(yearDimension);
            context.write(commonDimension,v);

            commonDimension.setDateDimension(monthDimension);
            context.write(commonDimension,v);

            commonDimension.setDateDimension(dayDimension);
            context.write(commonDimension,v);








        }
    }
    public static class CountDurationReducer extends Reducer<CommonDimension,Text,CommonDimension,CountDurationValue>{

        CountDurationValue countDurationValue = new CountDurationValue();

        @Override
        protected void reduce(CommonDimension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int counter = 0;
            int durations = 0;

            for(Text value:values){
                counter++;
                durations+= Integer.valueOf(value.toString());

            }
            countDurationValue.setCount(counter);
            countDurationValue.setDuration(durations);
            context.write(key,countDurationValue);

        }
    }
}
