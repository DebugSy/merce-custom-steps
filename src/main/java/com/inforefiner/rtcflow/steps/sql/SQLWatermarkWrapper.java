package com.inforefiner.rtcflow.steps.sql;

import com.merce.woven.annotation.Select;
import com.merce.woven.annotation.SelectType;
import com.merce.woven.annotation.Setting;
import com.merce.woven.annotation.SettingsContainer;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@SettingsContainer
public class SQLWatermarkWrapper {

    @Setting(description = "输入", select = Select.INPUT, selectType = SelectType.ID)
    private String input;

    @Setting(description = "时间列字段", select = Select.INPUT, selectType = SelectType.FIELDS)
    private String watermark;

    @Setting(description = "选择输入字段", select = Select.INPUT, selectType = SelectType.FIELDS)
    private String selectCol;

}
