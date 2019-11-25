package com.ctillnow.com.ctillnow.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/30 19:32
 * 4
 */
public class DateDimension extends BaseDimension{

    private String year;
    private String month;
    private String day;

    public DateDimension(String year, String month, String day) {
        this.year = year;
        this.month = month;
        this.day = day;
    }

    public DateDimension() {

    }

    @Override
    public String toString() {
        return year+"\t"+month+"\t"+day;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public int compareTo(BaseDimension o) {
        DateDimension dateDimension = (DateDimension) o;
        int result = this.year.compareTo(dateDimension.year);
        if(result==0){
            result = this.month.compareTo(dateDimension.month);
            if(result==0){
                return this.day.compareTo(dateDimension.day);
            }
        }

        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(month);
        dataOutput.writeUTF(day);

    }

    public void readFields(DataInput dataInput) throws IOException {
        this.year= dataInput.readUTF();
        this.month= dataInput.readUTF();
        this.day= dataInput.readUTF();

    }
}
