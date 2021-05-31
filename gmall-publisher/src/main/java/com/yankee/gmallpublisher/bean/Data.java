package com.yankee.gmallpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 数据
 * @date 2021/5/27 22:55
 */
@lombok.Data
@AllArgsConstructor
@NoArgsConstructor
public class Data {
    private List<String> categories;

    private Series[] series;
}
