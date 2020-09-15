# Platform平台SDK开发文档

## 基本概念
- @StepBind 该注解作用在Step定义的类上，参数id,name,type(三者一致即可),参数StepSetting指定该Step配置信息
- @UDF 该注解用在UDF定义的类上，参数表明UDF的名称
- @Setting 设置所需参数
  - defaultValue 设置参数的默认值
  - description 参数的描述信息
  - advanced 是否是高级选项，高级选项会隐藏在高级设置中
  - required 是够是必须项，如果是必须项，前端会检验是否为空
  
  - scope 参数作用域
  - scopeType 参数作用域的数据类型
  - bind 当scope设置后，scope的值与bind的值相同时才显示
  
  - format 格式化，目前支持"sql","add"。sql表示该参数需要使用sql样式渲染，add代表可添加
  - compositeStep 复杂Step，目前有Source,Sink,Lookup
  - order 参数排序，可调整显示的顺序
  - values 可选择的值，前端会显示成下拉框
  - select 该参数表明值的选择来源，即从何处选择
        
        select值 | 含义
        ---|---
        INPUT | 从INPUT中选择
        LEFT | 从LEFT中选择，如join step分左右输入的
        RIGHT | 从RIGHT中选择，如join step分左右输入的
        DATASET | 从数据集Dataset中选择
        SCHEMA | 从元数据Schema中选择
        ID | 从输入的多个ID中选择
        NULL | 不选择任何东西（默认值）
  - selectType 该参数表明选择类型,即选择什么数据
    
       selectType值 | 含义
       ---|---
       ID | 选择输入节点的ID
       FIELDS | 选择输入节点的输入字段
       SCHEMA | 选择一个元数据Schema
       DATASET | 选择一个数据集Dataset
       UDF | 选择一个已注册的UDF
       NULL | 不选择任何东西（默认值）

## 自定义Step开发
- Step开发应该继承类`com.merce.woven.flow.spark.flow.Step`
```java
@StepBind(id = "rtc_filter", settingClass = FilterSettings.class)
public class FilterStep extends Step<FilterSettings, DataStream<Row>> {

    public FilterStep() {
        this.stepCategory = StepCategory.TRANSFORM;
    }

    @Override
    public FilterSettings initSettings() {
        return new FilterSettings();
    }

    @Override
    public void setup() {
        //预处理
    }

    @Override
    public void process() {
        //数据处理
    }

    @Override
    public StepValidateResult validate() {
        //校验
    }

}

//以下是配置类信息
@Getter
@Setter
class FilterSettings extends StepSettings {

    @Setting(description = "key")
    int keyIndex = -1;

    @Setting(description = "key value")
    String keyValue;

}

```

## 自定义UDF开发 

```java

/**
 * Created by P0007 on 2020/04/15.
 */
@UDF(name = "WEEK_OF_YEAR_TEST")
public class WeekUDF extends ScalarFunction {

    /**
     * 获取timestamp在这一年中是第几周
     * @param timestamp
     * @return
     */
    public int eval(Timestamp timestamp) {
        LocalDateTime localDateTime = timestamp.toLocalDateTime();
        int week = localDateTime.get(WeekFields.ISO.weekOfWeekBasedYear());
        return week;
    }

}

```
