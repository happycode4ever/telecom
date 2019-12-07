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
public class DateDimension extends BaseDimension {
    private int id;
    private int year = -1;
    private int month = -1;
    private int day = -1;
    @Override
    public int compareTo(BaseDimension o) {
        DateDimension other = (DateDimension) o;
        int i1 = Integer.compare(this.year, other.year);
        if(i1!=0)return i1;
        int i2 = Integer.compare(this.month, other.month);
        if(i2!=0)return i2;
        int i3 = Integer.compare(this.day, other.day);
        return i3;

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(month);
        dataOutput.writeInt(day);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        month = dataInput.readInt();
        day = dataInput.readInt();
    }
}
