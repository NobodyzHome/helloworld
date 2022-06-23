package com.mzq.hello.flink.func.broadcast;

import com.mzq.hello.domain.ItemDeal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BroadcastProcessFun extends BroadcastProcessFunction<ItemDeal, Tuple2<String, Integer>, Tuple2<String, Double>> implements CheckpointedFunction {

    private ListState<ItemDeal> itemDealListState;
    private final MapStateDescriptor<String, Integer> mapStateDescriptor;

    public BroadcastProcessFun(MapStateDescriptor<String, Integer> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        itemDealListState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("history", Types.GENERIC(ItemDeal.class)));
    }

    @Override
    public void processBroadcastElement(Tuple2<String, Integer> value, BroadcastProcessFunction<ItemDeal, Tuple2<String, Integer>, Tuple2<String, Double>>.Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {
        // broadcast侧的数据可以更新和写入broadcast侧的数据可以更新和写入broadcast state
        ctx.getBroadcastState(mapStateDescriptor).put(value._1, value._2());
    }

    @Override
    public void processElement(ItemDeal value, BroadcastProcessFunction<ItemDeal, Tuple2<String, Integer>, Tuple2<String, Double>>.ReadOnlyContext ctx, Collector<Tuple2<String, Double>> out) throws Exception {
        // 非broadcast侧只能读取broadcast state
        Iterable<Map.Entry<String, Integer>> immutableEntries = ctx.getBroadcastState(mapStateDescriptor).immutableEntries();
        List<ItemDeal> list = new ArrayList<>();
        itemDealListState.get().forEach(list::add);

        // 先处理operator state中记录的没有获取到价格的交易数据，并处理
        if (!list.isEmpty()) {
            Iterator<ItemDeal> iterator = list.iterator();
            while (iterator.hasNext()) {
                ItemDeal history = iterator.next();
                for (Map.Entry<String, Integer> entry : immutableEntries) {
                    String itemName = entry.getKey();
                    Integer itemPrice = entry.getValue();

                    if (itemName.equals(history.getItemName())) {
                        out.collect(new Tuple2<>(itemName, BigDecimal.valueOf(history.getDealCount()).multiply(new BigDecimal(itemPrice)).setScale(2, RoundingMode.HALF_UP).doubleValue()));
                        iterator.remove();
                    }
                }
            }
        }

        // 再处理当前数据，去broadcast state中查询价格，如果有则处理并下发至下游，如果没有则存储到operator state中
        boolean has = false;
        for (Map.Entry<String, Integer> entry : immutableEntries) {
            String itemName = entry.getKey();
            Integer itemPrice = entry.getValue();

            if (itemName.equals(value.getItemName())) {
                BigDecimal itemCount = BigDecimal.valueOf(value.getDealCount());
                out.collect(new Tuple2<>(itemName, itemCount.multiply(new BigDecimal(itemPrice)).setScale(2, RoundingMode.HALF_UP).doubleValue()));
                has = true;
            }
        }
        if (!has) {
            list.add(value);
        }
        itemDealListState.update(list);
    }
}