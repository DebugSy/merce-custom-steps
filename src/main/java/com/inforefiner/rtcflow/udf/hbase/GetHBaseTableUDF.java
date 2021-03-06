package com.inforefiner.rtcflow.udf.hbase;

import com.merce.woven.annotation.Setting;
import com.merce.woven.annotation.UDF;
import com.nokia.bighead.hbase.RowKeyBuild;
import com.nokia.bighead.hbase.entity.HbaseTableDTO;
import com.nokia.bighead.hbase.exception.RowKeyException;
import com.nsn.bighead.roe.meta.webservice.entity.WebServiceEntity;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Date;

@UDF(name = "get_tablename")
@Slf4j
public class GetHBaseTableUDF extends ScalarFunction {

    /**
     * 外部接口相关参数，可以写死
     */
    private long connectionTimeout = 1800 * 1000;

    private long receiveTimeout = 360 * 1000;

    private String serviceName = "sourceService";

    private String serviceClass = "com.nsn.bighead.roe.meta.service.SourceService";

    /**
     * 注册函数的时候，指定访问的url 地址
     */
    @Setting(description = "ws URL")
    @Getter
    @Setter
    private String url;

    private WebServiceEntity wsEntity;

    @Override
    public void open(FunctionContext context) throws Exception {
        /**
         * 构造配置参数类
         */
        WebServiceEntity wsEntity = new WebServiceEntity();
        wsEntity.setConnectionTimeout(connectionTimeout);
        wsEntity.setReceiveTimeout(receiveTimeout);
        wsEntity.setServiceUrl(url);//ws 的url
        wsEntity.setServiceName(serviceName);
        wsEntity.setServiceClass(serviceClass);
        this.wsEntity = wsEntity;
    }

    /**
     *
     * @param tabNameEn
     * @param time
     */
    public String eval(String tabNameEn,Date time) {
        try {
            //获取表名方法调用
            HbaseTableDTO hbaseTableDTO = RowKeyBuild.getInstance(wsEntity).getHTableInfo(tabNameEn, time);
            return hbaseTableDTO.getTableName();
        } catch (RowKeyException e) {
            log.error("execute rowkey throw exception", e);
            throw new RuntimeException("execute rowkey throw exception", e);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
