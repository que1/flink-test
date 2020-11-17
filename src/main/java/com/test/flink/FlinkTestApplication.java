package com.test.flink;

import com.test.flink.frauddetection.FraudDetectionJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FlinkTestApplication {

    private static final Logger logger = LoggerFactory.getLogger(FlinkTestApplication.class);

    static {
        System.setProperty("Log4jContextSelector", "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector");
    }

    public static void main(String[] args) {
        SpringApplication.run(FlinkTestApplication.class, args);
        logger.info("start flink test");
        FraudDetectionJob fraudDetectionJob = new FraudDetectionJob();
        fraudDetectionJob.execute();
    }

}
