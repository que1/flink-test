package com.test.flink.frauddetection;

import com.test.flink.frauddetection.common.Alert;
import com.test.flink.frauddetection.common.AlertSlink;
import com.test.flink.frauddetection.common.Transaction;
import com.test.flink.frauddetection.common.TransactionSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FraudDetectionJob {

    private static final Logger logger = LoggerFactory.getLogger(FraudDetectionJob.class);

    public void execute() {
        // 设置执行环境
        // 任务执行环境用于定义任务的属性、创建数据源以及最终启动任务的执行
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建数据源
        // 数据源从外部系统例如Apache Kafka、Rabbit MQ或者Apache Pulsar接收数据，然后将数据送到Flink程序中
        DataStream<Transaction> transactions = env.addSource(new TransactionSource()).name("transactions");
        // 对事件分区
        // DataStream#keyBy方法对流进行分区，process函数对流绑定操作，这个操作会对流上的没一个消息调用定义好的函数
        DataStream<Alert> alerts = transactions.keyBy(Transaction::getAccountId).process(new FraudDetector()).name("fraud-detector");
        // 输出结果
        // sink会将DataStream写出到外部系统，例如Apache Kafka、Cassandra 或者 AWS Kinesis等
        alerts.addSink(new AlertSlink()).name("send-alerts");

        // 运行作业
        try {
            env.execute("fraud detection");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

}