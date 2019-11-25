package com.ctillnow.com.ctillnow.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/30 19:24
 * 4
 */
public class ContantDimension extends BaseDimension {

    private String phoneNum;
    private String name;


    @Override
    public String toString() {
        return phoneNum+"\t"+name;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int compareTo(BaseDimension o) {
        ContantDimension contantDimension = (ContantDimension)o;

        return this.phoneNum.compareTo(contantDimension.phoneNum);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.phoneNum);
        dataOutput.writeUTF(this.name);

    }

    public void readFields(DataInput dataInput) throws IOException {
        this.phoneNum= dataInput.readUTF();
        this.name = dataInput.readUTF();

    }
}
