package com.inforefiner.rtcflow.steps.sql;

import com.merce.woven.annotation.Setting;
import com.merce.woven.annotation.StepBind;
import com.merce.woven.flow.spark.flow.Step;
import com.merce.woven.flow.spark.flow.StepSettings;
import com.merce.woven.flow.spark.flow.StepValidateResult;
import com.merce.woven.step.StepCategory;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

/**
 * Created by P0007 on 2019/8/27.
 */
@StepBind(id = "rtc_sql", settingClass = SQLSettings.class)
public class SQLStep extends Step<SQLSettings, DataStream<Row>> {

    public SQLStep() {
        this.stepCategory = StepCategory.TRANSFORM;
    }

    @Override
    public SQLSettings initSettings() {
        return new SQLSettings();
    }

    @Override
    public void setup() {

    }

    @Override
    public void process() {

    }

    @Override
    public StepValidateResult validate() {
        StepValidateResult validationResult = new StepValidateResult(this.id);
        //no thing to do
        return validationResult;
    }
}

@Getter
@Setter
class SQLSettings extends StepSettings {

    @Setting(description = "sql语句", format = "sql")
    private String sql;

    @Setting(defaultValue = "false", description = "开启回撤功能", required = false, advanced = true)
    private boolean retract = false;

    @Setting(description = "设置Watermark", required = false, advanced = true)
    private SQLWatermarkWrapper[] sqlWatermarkWrapper;

}

