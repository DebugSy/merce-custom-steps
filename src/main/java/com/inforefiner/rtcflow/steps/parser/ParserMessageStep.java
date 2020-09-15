package com.inforefiner.rtcflow.steps.parser;

import com.inforefiner.rtcflow.util.ConvertUtil;
import com.merce.woven.annotation.SelectType;
import com.merce.woven.annotation.Setting;
import com.merce.woven.annotation.StepBind;
import com.merce.woven.common.SchemaMiniDesc;
import com.merce.woven.flow.spark.flow.Step;
import com.merce.woven.flow.spark.flow.StepSettings;
import com.merce.woven.step.StepCategory;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;

@StepBind(id = "rtc_parserMessage", settingClass = ParserMessageSettings.class)
public class ParserMessageStep extends Step<ParserMessageSettings, DataStream<Row>> {

    private SchemaMiniDesc schema;

    private RowTypeInfo rowTypeInfo;

    private ParserMessageFunction parserMessageFunction;

    public ParserMessageStep() {
        this.stepCategory = StepCategory.TRANSFORM;
        this.xtype = "ParserMessageStep";
    }

    public ParserMessageSettings initSettings() {
        return new ParserMessageSettings();
    }

    public void setup() {
        this.schema = settings.getSchema();
        this.rowTypeInfo = ConvertUtil.buildRowTypeInfo(schema.getFields());
        this.parserMessageFunction = new ParserMessageFunction(rowTypeInfo, settings);
    }

    public void process() {
        DataStream<Row> input = this.input();
        SingleOutputStreamOperator<Row> result = input
                .flatMap(parserMessageFunction)
                .returns(rowTypeInfo)
                .setParallelism(settings.getParallelism())
                .uid(this.id);
        this.addOutput(result);
    }
}

@Getter
@Setter
class ParserMessageSettings extends StepSettings {

    @Setting(description = "选择输出字段schema", selectType = SelectType.SCHEMA)
    private String schemaName;

    @Setting(defaultValue = "0", required = false, advanced = true, description = "Step并行度，如果是0则使用Job的并行度")
    private int parallelism = 0;

    @Setting(defaultValue = ",", required = false, advanced = true, description = "消息分隔符")
    private String splitRegex;

    @Setting(description = "标志位下标")
    private int dataFlagColumnIndex;

    @Setting(description = "标志位的值")
    private String[] dataFlagColumnValues;

    @Setting(defaultValue = ",", description = "值分隔符")
    private String dataFlagSeparator = ",";

    @Setting(defaultValue = ";", description = "字段分隔符")
    private String dataFlagColumnSeparator = ";";

    @Setting(defaultValue = ":", description = "名称与值分隔符")
    private String dataFlagKVSeparator = ":";

    @Setting(description = "忽略字段配置，示例: table1:1,3;table2:2,4")
    private String dataFlagIgnoreConfig;

    private SchemaMiniDesc schema;

}
