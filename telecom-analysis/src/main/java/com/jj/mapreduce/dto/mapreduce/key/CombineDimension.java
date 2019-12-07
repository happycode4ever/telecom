package com.jj.mapreduce.dto.mapreduce.key;

import lombok.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class CombineDimension extends BaseDimension {
    private ContactDimension contactDimension = new ContactDimension();
    private DateDimension dateDimension = new DateDimension();
    @Override
    public int compareTo(BaseDimension o) {
        //先比较时间再比较联系人信息
        CombineDimension other = (CombineDimension) o;
        int i1 = dateDimension.compareTo(other.dateDimension);
        if(i1!=0)return i1;
        return contactDimension.compareTo(other.contactDimension);
    }

    //直接调用子类的序列化和反序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        contactDimension.write(dataOutput);
        dateDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        contactDimension.readFields(dataInput);
        dateDimension.readFields(dataInput);
    }
}
