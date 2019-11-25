package com.ctillnow.com.ctillnow.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/30 19:32
 * 4
 */
public class CommonDimension extends BaseDimension{

    private ContantDimension contactDimension = new ContantDimension();
    private DateDimension dateDimension = new DateDimension();


    public ContantDimension getContactDimension() {
        return contactDimension;
    }

    public void setContactDimension(ContantDimension contactDimension) {
        this.contactDimension = contactDimension;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    @Override
    public String toString() {
        return contactDimension+"\t"+dateDimension;
    }

    public int compareTo(BaseDimension o) {
        CommonDimension commonDimension = (CommonDimension) o;
        int result = this.contactDimension.compareTo(commonDimension.contactDimension);
        if(result==0)
            result= this.dateDimension.compareTo(commonDimension.dateDimension);

        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.contactDimension.write(dataOutput);
        this.dateDimension.write(dataOutput);

    }

    public void readFields(DataInput dataInput) throws IOException {

        this.contactDimension.readFields(dataInput);
        this.dateDimension.readFields(dataInput);

    }
}
