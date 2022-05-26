package com.gzl0ng.func;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @author 郭正龙
 * @date 2021-12-03
 */

//自定义反序列化器，默认streamCDC流输出是二个toString的拼接
public class CustomerDeserializationSchema implements DebeziumDeserializationSchema<String> {

    //反序列化方法

    /**
     *
     * {
     *     "db":"",
     *     "table":"",
     *     "before":"{"id":"1001","name":""...}",
     *     "after":"{"id":"1001","name":""...}",
     *     "op":"",
     * }
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        /**
         * SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={transaction_id=null, ts_sec=1638496548, file=mysql-bin.000001, pos=548, row=1, server_id=1, event=2}} ConnectRecord{topic='mysql_binlog_source.cdc_test.user_info', kafkaPartition=null, key=Struct{id=1002}, keySchema=Schema{mysql_binlog_source.cdc_test.user_info.Key:STRUCT}, value=Struct{after=Struct{id=1002,name=lisi,sex=falme},source=Struct{version=1.5.2.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1638496548000,db=cdc_test,table=user_info,server_id=1,file=mysql-bin.000001,pos=694,row=0},op=c,ts_ms=1638496546639}, valueSchema=Schema{mysql_binlog_source.cdc_test.user_info.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
         */
        //创建JSON对象用于封装结果数据
        JSONObject result = new JSONObject();

        //获取库名&表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        result.put("db",fields[1]);
        result.put("tableName",fields[2]);

        //获取before数据
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null){
            //获取列信息
            Schema schema = before.schema();
            List<Field> fieldList = schema.fields();

            for (Field field : fieldList) {
                beforeJson.put(field.name(),before.get(field));
            }
        }
        result.put("before",beforeJson);

        //获取after数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null){
            //获取列信息
            Schema schema = after.schema();
            List<Field> fieldList = schema.fields();

            for (Field field : fieldList) {
                afterJson.put(field.name(),after.get(field));
            }
        }
        result.put("after",afterJson);

        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("op",operation);


        //输出数据
        collector.collect(result.toJSONString());
    }

    //获取类型
    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
