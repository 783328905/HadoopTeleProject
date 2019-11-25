package com.ctillnow.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/30 14:56
 * 4
 */

//生成多组start stop rows
public class HBaseScanFilter {
    public static List<String[]> list = new ArrayList<>();
    public static SimpleDateFormat simpleDateFormat ;

    public static List<String[]> getStartStop(String phoneNum,String start,String stop) throws ParseException {
        simpleDateFormat = new SimpleDateFormat("yyyy-MM");
        Date startDate = simpleDateFormat.parse(start);
        Date endDate = simpleDateFormat.parse(stop);
        Calendar startPoint =Calendar.getInstance();
        startPoint.setTime(startDate);


        Calendar stopPoint = Calendar.getInstance();
        stopPoint.setTime(startDate);
        stopPoint.add(Calendar.MONTH,1);

        while (startPoint.getTimeInMillis() <= endDate.getTime()){
            String buildTime = simpleDateFormat.format(startPoint.getTime());
            String stopTime = simpleDateFormat.format(stopPoint.getTime());
            String rowHash = HBaseUtil.getRowHash(6,phoneNum,buildTime);
            String startRow = rowHash+"_"+phoneNum+"_"+buildTime;
            String stopRow = rowHash+"_"+phoneNum+"_"+stopTime;

            list.add(new String[]{startRow,stopRow});
            startPoint.add(Calendar.MONTH,1);
            stopPoint.add(Calendar.MONTH,1);

        }
        return list;

    }

    private static int i = 0;

    public static boolean hasNext() {
        return i < list.size();
    }

    public static String[] next() {
        return list.get(i++);
    }
    public static void main(String[] args) {


    }
}
