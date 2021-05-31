package com.yankee.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.yankee.gmallpublisher.bean.*;
import com.yankee.gmallpublisher.service.KeyWordStatsService;
import com.yankee.gmallpublisher.service.ProductStatsService;
import com.yankee.gmallpublisher.service.ProvinceStatsService;
import com.yankee.gmallpublisher.service.VisitorStatsService;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.*;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description Suger
 * @date 2021/5/24 14:05
 */
@RestController
@RequestMapping("/api/sugar")
public class SugerController {
    @Autowired
    ProductStatsService productStatsService;

    @Autowired
    KeyWordStatsService keyWordStatsService;

    @Autowired
    ProvinceStatsService provinceStatsService;

    @Autowired
    VisitorStatsService visitorStatsService;

    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = now();
        }
        return "{\"status\": 0, \"msg\": \"\", \"data\": " + productStatsService.getGMV(date) + "}";
    }

    @RequestMapping("/trademark")
    public String getSumAmountByTmName(@RequestParam(value = "date", defaultValue = "0") Integer date,
                                       @RequestParam(value = "limit", defaultValue = "5") Integer limit) {
        if (date == 0) {
            date = now();
        }

        List<ProductStats> dataList = productStatsService.getSumAmountByTmName(date, limit);

        // 创建集合用于存放品牌名称，销售额
        ArrayList<String> tmList = new ArrayList<>();
        ArrayList<BigDecimal> amountList = new ArrayList<>();

        // 遍历dataList
        for (ProductStats productStats : dataList) {
            tmList.add(productStats.getTm_name());
            amountList.add(productStats.getOrder_amount());
        }

        Collections.reverse(tmList);
        Collections.reverse(amountList);

        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": { " +
                "    \"categories\": [ " +
                "\"" + StringUtils.join(tmList, "\",\"") + "\"" +
                "    ], " +
                "    \"series\": [ " +
                "      { " +
                "        \"name\": \"品牌\", " +
                "        \"data\": [ " +
                "\"" + StringUtils.join(amountList, "\",\"") + "\"" +
                "        ] " +
                "      } " +
                "    ] " +
                "  } " +
                "}";
    }

    @RequestMapping(value = "/keyword")
    public String getKeyWordStats(@RequestParam(value = "date", defaultValue = "0") Integer date,
                                  @RequestParam(value = "limit", defaultValue = "20") Integer limit) {
        if (date == 0) {
            date = now();
        }
        // 查询数据
        List<KeyWordStats> keyWordStatsList = keyWordStatsService.selectKeyWordStats(date, limit);
        StringBuilder jsonSb = new StringBuilder("{\"status\": 0, \"msg\": \"\", \"data\": [");
        // 循环拼接字符串
        if (!keyWordStatsList.isEmpty()) {
            for (int i = 0; i < keyWordStatsList.size(); i++) {
                KeyWordStats keyWordStats = keyWordStatsList.get(i);
                if (i >= 1) {
                    jsonSb.append(",");
                }
                jsonSb.append("{\"name\": \"").append(keyWordStats.getWord()).append("\", \"value\": ").append(keyWordStats.getWord_count()).append("}");
            }
        }
        jsonSb.append("]}");
        return jsonSb.toString();
    }

    @RequestMapping("/hr")
    public String getVisitorStatsByHr(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = now();
        }
        // 从service层访问数据
        List<VisitorStats> visitorStatsList = visitorStatsService.selectVisitorStatsByHr(date);
        // 创建数据，保存每小时的数据
        VisitorStats[] visitorStatsArr = new VisitorStats[24];
        for (VisitorStats visitorStats : visitorStatsList) {
            visitorStatsArr[visitorStats.getHr()] = visitorStats;
        }
        // 定义存放每小时、uv、pv、新用户的List集合
        ArrayList<String> hrList = new ArrayList<>();
        ArrayList<Long> uvList = new ArrayList<>();
        ArrayList<Long> pvList = new ArrayList<>();
        ArrayList<Long> newVisitorList = new ArrayList<>();

        // 对数据遍历
        for (int i = 0; i <= 23; i++) {
            VisitorStats visitorStats = visitorStatsArr[i];
            if (visitorStats != null) {
                uvList.add(visitorStats.getUv_ct());
                pvList.add(visitorStats.getPv_ct());
                newVisitorList.add(visitorStats.getNew_uv());
            } else {
                uvList.add(0L);
                pvList.add(0L);
                newVisitorList.add(0L);
            }
            // 小时位不足2位时，补零
            hrList.add(String.format("%02d", i));
        }

        Series[] series = new Series[3];
        Series uvSeries = new Series();
        uvSeries.setName("uv");
        uvSeries.setData(uvList);
        series[0] = uvSeries;
        Series pvSeries = new Series();
        pvSeries.setName("pv");
        pvSeries.setData(pvList);
        series[1] = pvSeries;
        Series newSeries = new Series();
        newSeries.setName("新用户");
        newSeries.setData(newVisitorList);
        series[2] = newSeries;
        VisitorResult visitorResult = new VisitorResult(0, "", new Data(hrList, series));

        return JSONObject.toJSONString(visitorResult);
        // return "{\"status\": 0, \"msg\": \"\", \"data\": {\"categories\": [\""
        //         + StringUtils.join(hrList, "\", \"") + "\"], \"series\": [{\"name\": \"uv\", \"data\": ["
        //         + StringUtils.join(uvList, ",") + "]}, {\"name\": \"pv\", \"data\": ["
        //         + StringUtils.join(pvList, ",") + "]}, {\"name\": \"新用户\", \"data\": ["
        //         + StringUtils.join(newVisitorList, ",") + "]}]}}";
    }

    @RequestMapping("/visitor")
    public Map getVisitorStatsByNameFlag(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = now();
        }
        List<VisitorStats> visitorStatsList = visitorStatsService.selectVisitorStatsByNewFlag(date);
        // 定义对象，接收新老访客的统计结果
        VisitorStats newVisitorStats = new VisitorStats();
        VisitorStats oldVisitorStats = new VisitorStats();

        for (VisitorStats visitorStats : visitorStatsList) {
            if ("1".equals(visitorStats.getIs_new())) {
                newVisitorStats = visitorStats;
            } else {
                oldVisitorStats = visitorStats;
            }
        }
        // 返回json字符串处理
        HashMap resMap = new HashMap();
        resMap.put("status", 0);
        resMap.put("msg", "");
        HashMap dataMap = new HashMap();
        dataMap.put("combineNum", 1);

        // 表头
        ArrayList columnList = new ArrayList();
        HashMap typeHeader = new HashMap();
        typeHeader.put("name", "类别");
        typeHeader.put("id", "type");
        columnList.add(typeHeader);

        HashMap newHeader = new HashMap();
        newHeader.put("name", "新用户");
        newHeader.put("id", "new");
        columnList.add(newHeader);

        HashMap oldHeader = new HashMap();
        oldHeader.put("name", "老用户");
        oldHeader.put("id", "old");
        columnList.add(oldHeader);
        dataMap.put("columns", columnList);

        // 表格
        ArrayList rowList = new ArrayList();
        // 用户数
        HashMap userCount = new HashMap();
        userCount.put("type", "用户数（人）");
        userCount.put("new", newVisitorStats.getUv_ct());
        userCount.put("old", oldVisitorStats.getUv_ct());
        rowList.add(userCount);

        // 总访问页面数
        HashMap pageTotal = new HashMap();
        pageTotal.put("type", "总访问页面（次）");
        pageTotal.put("new", newVisitorStats.getPv_ct());
        pageTotal.put("old", oldVisitorStats.getPv_ct());
        rowList.add(pageTotal);

        // 跳出率
        HashMap jumpRate = new HashMap();
        jumpRate.put("type", "跳出率（%）");
        jumpRate.put("new", newVisitorStats.getPvPerSv());
        jumpRate.put("old", oldVisitorStats.getPvPerSv());
        rowList.add(jumpRate);

        // 平均在线时长
        HashMap ageDurTime = new HashMap();
        ageDurTime.put("type", "平均在线时长（秒）");
        ageDurTime.put("new", newVisitorStats.getDurPerSv());
        ageDurTime.put("old", oldVisitorStats.getDurPerSv());
        rowList.add(ageDurTime);

        // 平均页面访问人数
        HashMap ageVisitorCount = new HashMap();
        ageVisitorCount.put("type", "平均访问人数（人次）");
        ageVisitorCount.put("new", newVisitorStats.getPvPerSv());
        ageVisitorCount.put("old", oldVisitorStats.getPvPerSv());
        rowList.add(ageVisitorCount);

        dataMap.put("rows", rowList);
        resMap.put("data", dataMap);
        return resMap;
    }

    @RequestMapping("/province")
    public Map getProvinceStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = now();
        }
        List<ProvinceStats> provinceStatsList = provinceStatsService.selectProvinceStats(date);

        // 拼接json
        HashMap resMap = new HashMap();
        resMap.put("status", 0);
        resMap.put("msg", "");
        HashMap dataMap = new HashMap();
        dataMap.put("valueName", "金额");

        ArrayList mapData = new ArrayList();
        for (int i = 0; i < provinceStatsList.size(); i++) {
            ProvinceStats provinceStats = provinceStatsList.get(i);
            HashMap maps = new HashMap();
            maps.put("name", provinceStats.getProvince_name());
            maps.put("value", provinceStats.getOrder_amount());
            mapData.add(maps);
        }

        dataMap.put("mapData", mapData);
        resMap.put("data", dataMap);
        return resMap;
    }

    @RequestMapping("/spu")
    public Map getProvinceStatsBySpu(@RequestParam(value = "date", defaultValue = "0") Integer date,
                                        @RequestParam(value = "limit", defaultValue = "10") Integer limit) {
        if (date == 0) {
            date = now();
        }
        List<ProductStats> productStatsBySpuList = productStatsService.getProductStatsBySpu(date, limit);
        // 初始化表头信息
        HashMap resMap = new HashMap();
        resMap.put("status", 0);
        resMap.put("msg", "");
        HashMap dataMap = new HashMap();

        // 表头
        ArrayList columnList = new ArrayList();
        HashMap spuMap = new HashMap();
        spuMap.put("name", "商品SPU名称");
        spuMap.put("id", "spu_name");
        columnList.add(spuMap);

        HashMap orderAmountMap = new HashMap();
        orderAmountMap.put("name", "交易额");
        orderAmountMap.put("id", "order_amount");
        columnList.add(orderAmountMap);

        HashMap orderCountMap = new HashMap();
        orderCountMap.put("name", "订单数");
        orderCountMap.put("id", "order_ct");
        columnList.add(orderCountMap);

        // 内容
        ArrayList rowList = new ArrayList();
        for (int i = 0; i < productStatsBySpuList.size(); i++) {
            ProductStats productStats = productStatsBySpuList.get(i);
            HashMap rowMap = new HashMap();
            rowMap.put("spu_name", productStats.getSpu_name());
            rowMap.put("order_amount", productStats.getOrder_amount());
            rowMap.put("order_ct", productStats.getOrder_ct());
            rowList.add(rowMap);
        }

        dataMap.put("columns", columnList);
        dataMap.put("rows", rowList);
        resMap.put("data", dataMap);
        return resMap;
    }

    @RequestMapping("/category3")
    public Map getProductStatsByCategory3(@RequestParam(value = "date", defaultValue = "0") Integer date,
                                          @RequestParam(value = "limit", defaultValue = "5") Integer limit) {
        if (date == 0) {
            date = now();
        }
        List<ProductStats> productStatsByCategory3List = productStatsService.getProductStatsByCategory3(date, limit);
        HashMap resMap = new HashMap();
        resMap.put("status", 0);
        resMap.put("msg", "");
        ArrayList dataList = new ArrayList();
        for (ProductStats productStats : productStatsByCategory3List) {
            HashMap dataMap = new HashMap();
            dataMap.put("name", productStats.getCategory3_name());
            dataMap.put("value", productStats.getOrder_amount());
            dataList.add(dataMap);
        }

        resMap.put("data", dataList);
        return resMap;
    }

    /**
     * 获取当前时间
     *
     * @return 返回当前时间
     */
    private int now() {
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.parseInt(yyyyMMdd);
    }
}
