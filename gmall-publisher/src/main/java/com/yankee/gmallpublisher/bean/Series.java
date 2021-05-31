package com.yankee.gmallpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description
 * @date 2021/5/27 23:32
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Series {
    private String name;

    private List<Long> data;
}
