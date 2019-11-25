
import java.io.*;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/22 14:23
 * 4
 */

public class ProducerLog {
    private static String[] telFirst = "134,135,136,137,138,139,150,151,152,157,158,159,130,131,132,155,156,133,153".split(",");
    //两个电话号码，开始通话时间
    //随机创建通话时长

     int getNum(int start,int end){
         Random random = new Random();
         return random.nextInt((end-start+1))+start;

    }
    public String productPhoneNumber(){
         String first = telFirst[(int)(Math.random()*telFirst.length)];
         int second = getNum(1000,9000);
         int third = getNum(1000,9100);
         return first+second+third;

    }
    public String randomDate(String startDate,String endDate){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date start = dateFormat.parse(startDate);
            Date end = dateFormat.parse(endDate);
            if(start.getTime()-end.getTime()>0){
                return null;
            }
            long result = start.getTime()+ (long)(Math.random()*(end.getTime()-start.getTime()));
            return result+"";
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;


    }
    public String productLog() throws ParseException {
         Random random = new Random();
         String phone1 = productPhoneNumber();
         String phone2 = productPhoneNumber();
         while (phone1.equals( phone2)){
             phone2=productPhoneNumber();
         }

         //打电话日期
         String dateStr = randomDate("2018-01-01","2019-07-10");
         SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String format = simpleDateFormat.format(Long.parseLong(dateStr));
        //通话时间
         int seconds = random.nextInt(30*60)+1;
         String secondsString  = new DecimalFormat("0000").format(seconds);

         StringBuffer stringBuffer = new StringBuffer();
         stringBuffer.append(phone1).append(",")
                 .append(phone2).append(",")
                 .append(format).append(",")
                 .append(secondsString);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return stringBuffer.toString();


    }
    public void writeLog(String filePath ,ProducerLog producerLog)  {
        OutputStreamWriter outputStreamWriter= null;
         try {
             while (true) {
                 outputStreamWriter = new OutputStreamWriter(new FileOutputStream(filePath,true),"UTF-8");
                 outputStreamWriter.write(producerLog.productLog() + "\n");
                 outputStreamWriter.flush();
             }
         }catch (Exception e){
             e.printStackTrace();
         }finally {
             try {
                 outputStreamWriter.flush();
                 outputStreamWriter.close();
             } catch (IOException e) {
                 e.printStackTrace();
             }

         }


    }

    public static void main(String args[]) throws ParseException {
        ProducerLog producerLog = new ProducerLog();
        if(args==null||args.length<=0){
            System.out.println("无参数");
            System.exit(1);

        }
        producerLog.writeLog(args[0],producerLog);


    }

}
