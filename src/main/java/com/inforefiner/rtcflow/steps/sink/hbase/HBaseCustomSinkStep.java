package com.inforefiner.rtcflow.steps.sink.hbase;

import com.merce.woven.annotation.Select;
import com.merce.woven.annotation.SelectType;
import com.merce.woven.annotation.Setting;
import com.merce.woven.annotation.StepBind;
import com.merce.woven.flow.spark.flow.Step;
import com.merce.woven.flow.spark.flow.StepSettings;
import com.merce.woven.flow.spark.flow.StepValidateResult;
import com.merce.woven.step.StepCategory;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

@StepBind(id = "custom_hbase_sink", settingClass = HBaseCustomSinkSettings.class)
public class HBaseCustomSinkStep extends Step<HBaseCustomSinkSettings, DataStream<Row>> {

    private String tableColumn;

    private String rowkeyColumn;

    private Map<String, String> columnFamilyMap = new HashMap<>();

    private long bufferMaxSize;
    private long bufferMaxRows;

    private long cacheExpireSecond;

    public HBaseCustomSinkStep() {
        this.stepCategory = StepCategory.SINK;
    }

    @Override
    public HBaseCustomSinkSettings initSettings() {
        return new HBaseCustomSinkSettings();
    }

    @Override
    public void setup() {
        this.tableColumn = settings.getTableColumn();
        this.rowkeyColumn = settings.getRowKeyColumn();
        String columns = settings.getColumns();
        String[] arr = columns.split(",");
        for (int i = 0; i < arr.length; i++) {
            String[] arr2 = arr[i].split(":");
            String k = arr2[0];
            String v = arr2[1];
            if (!k.equalsIgnoreCase("rowKey")) {
                this.columnFamilyMap.put(v, k);
            }
        }

        this.bufferMaxRows = settings.getBatchRows();
        this.bufferMaxSize = settings.getBatchSize();
        this.cacheExpireSecond = settings.getCacheExpireSecond();
    }

    @Override
    public void process() {
        DataStream<Row> input = this.input();
        RowTypeInfo rowTypeInfo = (RowTypeInfo) input.getType();
        int rowKeyIdx = rowTypeInfo.getFieldIndex(rowkeyColumn);
        HBaseCustomSinkFunction hBaseCustomSinkFunction = new HBaseCustomSinkFunction(
                tableColumn,
                rowKeyIdx,
                columnFamilyMap,
                rowTypeInfo,
                bufferMaxSize,
                bufferMaxRows,
                cacheExpireSecond);

        input.addSink(hBaseCustomSinkFunction)
                .setParallelism(settings.getParallelism())
                .uid(this.id)
                .name(this.id);
    }

    @Override
    public StepValidateResult validate() {
        StepValidateResult validateResult = new StepValidateResult(this.id);
        return validateResult;
    }
}

@Getter
@Setter
class HBaseCustomSinkSettings extends StepSettings {

    @Setting(description = "Step并行度，如果是0则使用Job的并行度", defaultValue = "0", required = false, advanced = true)
    private int parallelism = 0;

    @Setting(description = "描述HBase表名的列(全名称表名)", select = Select.INPUT, selectType = SelectType.FIELDS)
    private String tableColumn;

    @Setting(description = "描述HBase主键的列", select = Select.INPUT, selectType = SelectType.FIELDS)
    private String rowKeyColumn;

    @Setting(description = "列簇描述")
    private String columns;

    @Setting(description = "最大缓存数据大小,默认2M", defaultValue = "2097152", advanced = true, required = false)
    private long batchSize = 2097152L;

    @Setting(description = "最大缓存数据条数,默认-1无限制", defaultValue = "-1", advanced = true, required = false)
    private long batchRows = -1;

    @Setting(description = "HTable最长生存时间(秒)", defaultValue = "3600")
    private long cacheExpireSecond;

}
