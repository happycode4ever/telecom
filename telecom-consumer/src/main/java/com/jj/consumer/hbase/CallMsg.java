package com.jj.consumer.hbase;

import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class CallMsg {
    private String call1;
    private String call2;
    private String buildTime;
    private String duration;
}
