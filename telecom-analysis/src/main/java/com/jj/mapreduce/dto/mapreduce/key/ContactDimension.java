package com.jj.mapreduce.dto.mapreduce.key;

import lombok.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ContactDimension extends BaseDimension {
    private int id;
    private String telephone;
    private String name;
    @Override
    public int compareTo(BaseDimension o) {
        ContactDimension other = (ContactDimension) o;
        int i1 = this.telephone.compareTo(other.telephone);
        if(i1!=0)return i1;
        return this.name.compareTo(other.name);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(telephone);
        dataOutput.writeUTF(name);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        telephone = dataInput.readUTF();
        name = dataInput.readUTF();
    }
}
