package com.yankee.gmall.realtime.app.fun;

import com.yankee.gmall.realtime.common.GmallConstant;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 统计商品行为关键词
 * @date 2021/5/21 15:32
 */
@FunctionHint(output = @DataTypeHint("ROW<word_count BIGINT, source STRING>"))
public class KeyWordProductUDTF extends TableFunction<Row> {
    public void eval(Long click_count, Long cart_count, Long order_count) {
        if (click_count > 0L) {
            Row rowClick = new Row(2);
            rowClick.setField(0, click_count);
            rowClick.setField(1, GmallConstant.KEYWORD_CLICK);
            collect(rowClick);
        }

        if (cart_count > 0L) {
            Row rowCart = new Row(2);
            rowCart.setField(0, cart_count);
            rowCart.setField(1, GmallConstant.KEYWORD_CART);
            collect(rowCart);
        }

        if (order_count > 0L) {
            Row rowOrder = new Row(2);
            rowOrder.setField(0, order_count);
            rowOrder.setField(1, GmallConstant.KEYWORD_ORDER);
            collect(rowOrder);
        }
    }
}
