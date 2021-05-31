package com.yankee.gmallpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 访客主题结果
 * @date 2021/5/27 22:54
 */
@lombok.Data
@AllArgsConstructor
@NoArgsConstructor
public class VisitorResult {
    private Integer status;

    private String msg;

    private Data data;
}
