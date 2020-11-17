package com.test.flink.frauddetection;

import com.sun.org.apache.xpath.internal.operations.Bool;
import com.test.flink.frauddetection.common.Alert;
import com.test.flink.frauddetection.common.Transaction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 *
 *
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = -8613993152345050390L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<Boolean>("flag", Types.BOOLEAN);
        this.flagState = this.getRuntimeContext().getState(flagDescriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<Long>("timer-state", Types.LONG);
        this.timerState = this.getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {
        // get the current state for current key
        Boolean lastTransactionWasSmall = this.flagState.value();
        // check if the flag is set
        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > FraudDetector.LARGE_AMOUNT) {
                // output an alert downstream
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());

                collector.collect(alert);
            }
            // clean up our state
            this.flagState.clear();
        }

        if (transaction.getAmount() < FraudDetector.SMALL_AMOUNT) {
            // set the flag to true
            this.flagState.update(true);
            // set the timer state
            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(timer);
            this.timerState.update(timer);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        // remove flag after 1 minute
        this.timerState.clear();
        this.flagState.clear();
    }

    private void cleanUp(Context ctx) throws Exception {
        // delete timer
        Long timer = this.timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);

        // clean up all state
        this.timerState.clear();
        this.flagState.clear();
    }
}
