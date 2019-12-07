package com.jj.mapreduce.dto.mapreduce.value;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AnalysisResult extends BaseValue {
    private int callSum;
    private int durationSum;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(callSum);
        dataOutput.writeInt(durationSum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        callSum = dataInput.readInt();
        durationSum = dataInput.readInt();
    }
}
