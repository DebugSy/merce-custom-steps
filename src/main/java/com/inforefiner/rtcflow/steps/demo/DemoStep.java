package com.inforefiner.rtcflow.steps.demo;

import com.inforefiner.rtcflow.steps.sql.SQLWatermarkWrapper;
import com.inforefiner.rtcflow.util.ConvertUtil;
import com.merce.woven.annotation.Select;
import com.merce.woven.annotation.SelectType;
import com.merce.woven.annotation.Setting;
import com.merce.woven.annotation.StepBind;
import com.merce.woven.common.FieldDesc;
import com.merce.woven.common.StepFieldGroup;
import com.merce.woven.flow.spark.flow.Step;
import com.merce.woven.flow.spark.flow.StepSettings;
import com.merce.woven.flow.spark.flow.StepValidateResult;
import com.merce.woven.step.StepCategory;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * id: 全局唯一的step id,不可重复
 * settingClass: 代表前端展示的配置从这个class属性获取
 * inputCount: 输入节点个数
 * inputIds: 输入节点的key列表
 * outputCount: 输出节点个数
 * outputIds: 输出节点的key列表
 *
 * 整个Step的生命周期如下：
 * 1. 构造函数
 * 2. initSettings()
 * 3. setup()
 * 4. process()
 *
 * 以下函数会在前端配置时调用：
 * 5. fields()
 * 6. validate()
 */
@StepBind(id = "rtc_demo", settingClass = DemoSettings.class,
        inputCount = 2, inputIds = {"input1", "input2"},
        outputCount = 2, outputIds = {"output1", "output2"})
public class DemoStep extends Step<DemoSettings, DataStream<Row>> {

    private final static String INPUT_1 = "input1";
    private final static String INPUT_2 = "input2";
    private final static String OUTPUT_1 = "output1";
    private final static String OUTPUT_2 = "output2";

    /**
     * 构造函数
     */
    public DemoStep() {
        this.stepCategory = StepCategory.TRANSFORM;
    }

    /**
     * 返回配置类
     * @return
     */
    @Override
    public DemoSettings initSettings() {
        return new DemoSettings();
    }

    /**
     * 预处理
     */
    @Override
    public void setup() {

    }

    /**
     * 数据处理
     */
    @Override
    public void process() {
        logger.info("process step:" + this.getId());
        //1. 获取该Step的输入
        DataStream<Row> input1 = this.input(INPUT_1);
        RowTypeInfo rowTypeInfo1 = (RowTypeInfo) input1.getType();

        DataStream<Row> input2 = this.input(INPUT_2);
        RowTypeInfo rowTypeInfo2 = (RowTypeInfo) input2.getType();

        DataStream<Row> allInputStream = input1.union(input2);

        //新增字段num，代表当前task处理的记录数
        TypeInformation<Row> newRowTypeInfo1 = Types.ROW_NAMED(
                ArrayUtils.add(rowTypeInfo1.getFieldNames(), "num"),
                ArrayUtils.add(rowTypeInfo1.getFieldTypes(), Types.LONG)
        );

        //新增字段date，代表当前处理记录的时间
        TypeInformation<Row> newRowTypeInfo2 = Types.ROW_NAMED(
                ArrayUtils.add(rowTypeInfo2.getFieldNames(), "cur_date"),
                ArrayUtils.add(rowTypeInfo2.getFieldTypes(), Types.STRING)
        );

        //2. 对数据进行自定义的处理
        //2.1 新增记录数num字段
        SingleOutputStreamOperator<Row> resultStream1 = allInputStream.map(new MapFunction<Row, Row>() {
            private final Row row = new Row(1);
            private long num = 0;

            @Override
            public Row map(Row value) throws Exception {
                row.setField(0, ++num);
                Row result = Row.join(value, row);
                return result;
            }
        }).returns(newRowTypeInfo1);

        //2.2 新增时间date字段
        SingleOutputStreamOperator<Row> resultStream2 = allInputStream.map(new MapFunction<Row, Row>() {
            private final Row row = new Row(1);
            private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            @Override
            public Row map(Row value) throws Exception {
                long currentTimeMillis = System.currentTimeMillis();
                Date date = new Date(currentTimeMillis);
                row.setField(0, dateFormat.format(date));
                Row result = Row.join(value, row);
                return result;
            }
        }).returns(newRowTypeInfo2);

        //3. 将处理结果添加到输出，以便下游Step获取数据
        this.addOutput(OUTPUT_1, resultStream1);
        this.addOutput(OUTPUT_2, resultStream2);
    }

    /**
     * 输出字段处理
     *
     * 1. 获取输入字段
     *      a. inputFields() 获取输入key是"input"的所有字段
     *      b. inputFields("key") 获取指定输入key名称的所有字段
     *      c. getInputConfigurations() 获取所有的输入字段组
     *
     * 2. 设置输出字段
     *      a. addOutputFields(outputFields) 设置输出字段是outputFields字段列表,输出key是"output"
     *      b. addOutputFields("key", outputFields) 指定输出key名称
     *
     * @return 返回字段组
     */
    @Override
    public StepFieldGroup fields() {
        /*
        * 获取input输入字段,相当于调用inputFields("input")
        * 获取所有输入字段，使用StepFieldGroup inputFieldGroup = this.getInputConfigurations()
        * */
        StepFieldGroup stepFieldGroup = new StepFieldGroup();

        List<FieldDesc> output1Fields = new ArrayList<>();
        List<FieldDesc> input1Fields = this.inputFields(INPUT_1);
        output1Fields.addAll(input1Fields);
        output1Fields.add(new FieldDesc("num", "bigint", ""));
        stepFieldGroup.put(OUTPUT_1, output1Fields);

        List<FieldDesc> output2Fields = new ArrayList<>();
        List<FieldDesc> input2Fields = this.inputFields(INPUT_2);
        output2Fields.addAll(input2Fields);
        output2Fields.add(new FieldDesc("cur_date", "string", ""));
        stepFieldGroup.put(OUTPUT_2, output2Fields);

        return stepFieldGroup;
    }

    /**
     * 校验，前端点击Step确定的时候会调用此函数
     * @return
     */
    @Override
    public StepValidateResult validate() {
        StepValidateResult validationResult = new StepValidateResult(this.id);
        if (settings.getIntSetting() == -1) {
            validationResult.addError("condition", "condition setting is empty");
        }
        return validationResult;
    }
}

/**
 * Step参数类
 * 1. 支持int,long,string,boolean类型参数的UI自动化展示
 * 2. 支持复杂表单
 * 3. 如果是高级选项会隐藏在`高级选项`中，默认不展示
 */
@Getter
@Setter
class DemoSettings extends StepSettings {

    /**
     * 普通类型，UI展示为输入框
     */
    @Setting(description = "int", defaultValue = "-1")
    int intSetting = -1;

    /**
     * 普通类型，UI展示为输入框
     */
    @Setting(description = "long", defaultValue = "1000")
    long longSetting = 1000;

    /**
     * 普通类型，UI展示为输入框
     */
    @Setting(description = "string", defaultValue = "default")
    String stringSetting = "default";

    /**
     * 普通类型，UI展示为按钮
     */
    @Setting(description = "boolean", defaultValue = "false")
    boolean booleanSetting = false;

    /**
     * 选择输入节点的主键(即上一个节点输出字段的key,如output),UI展示为下拉选择
     */
    @Setting(description = "从输入中选择输入的主键", select = Select.INPUT, selectType = SelectType.ID)
    String selectInputId;

    /**
     * 选择字段,UI展示为下拉选择
     */
    @Setting(description = "从输入中选择字段", select = Select.INPUT, selectType = SelectType.FIELDS)
    String selectInputCol;

    /**
     * 复杂表单
     * UI展示如下：
     * sqlWatermarkWrapper: |-------input-----|----watermark----|---selectCol-----|
     *                      |                 |                 |                 |
     *                      |-----------------|-----------------|-----------------|
     */
    @Setting(description = "设置Watermark", required = false, advanced = true)
    private SQLWatermarkWrapper[] sqlWatermarkWrapper;

    /**
     * 设置固定的可选参数,UI展示为下拉选择
     */
    @Setting(defaultValue = "csv", values = {"csv", "json", "string"}, description = "数据格式")
    private String format = "csv";

    /**
     * 设置当scope指向的值是bind时才展示，即当format下拉选择csv时展示separator参数
     */
    @Setting(defaultValue = ",", scope = "format", bind = "csv", description = "CSV格式的分隔符")
    private String separator = ",";

    /**
     * 设置非必填参数,高级参数
     */
    @Setting(defaultValue = "", advanced = true, required = false, description = "CSV格式的空值")
    private String nullValue = "";

}
