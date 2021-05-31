package com.yankee.gmallpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 关键词主题
 * @date 2021/5/25 11:15
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeyWordStats {
    private String stt;
    private String edt;
    private String word;
    private Long word_count;
    private String ts;
}
