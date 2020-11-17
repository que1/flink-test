package com.test.flink.frauddetection.common;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertSlink implements SinkFunction<Alert> {

    private static final Logger logger = LoggerFactory.getLogger(AlertSlink.class);

    @Override
    public void invoke(Alert value, Context context) {
        logger.info(value.toString());
    }

}
