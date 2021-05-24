package com.yankee.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 搜索关键词宽表
 * @date 2021/5/21 11:16
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class KeyWordStats {
    private String stt;
    private String edt;
    private String source;
    private String word;
    private Long word_count;
    private Long ts;
}
