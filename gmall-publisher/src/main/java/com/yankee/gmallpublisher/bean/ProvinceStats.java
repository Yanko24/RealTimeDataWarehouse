package com.yankee.gmallpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 省份地区主题
 * @date 2021/5/25 11:16
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProvinceStats {
    private String stt;
    private String edt;
    private String province_id;
    private String province_name;
    private BigDecimal order_amount;
    private String ts;
}
