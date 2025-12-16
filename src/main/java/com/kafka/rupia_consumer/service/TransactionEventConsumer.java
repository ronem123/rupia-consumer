package com.kafka.rupia_consumer.service;


import com.rupia.kafa.KafkaTopics;
import com.rupia.kafa.TransactionEvent;
import com.rupia.kafa.TransferMoneyEvent;
import com.rupia.kafa.WalletReloadEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Created by Ram Mandal on 16/12/2025
 *
 * @System: Apple M1 Pro
 */

@Service
public class TransactionEventConsumer {

    @KafkaListener(topics = KafkaTopics.TRANSACTION_TOPIC, groupId = "rupia-transaction-group")
    public void consume(TransactionEvent event) {
        switch (event) {
            case WalletReloadEvent reload -> handleWalletReload(reload);
            case TransferMoneyEvent transfer -> handleTransferMoney(transfer);
            default -> throw new IllegalStateException("Unknown event type: " + event.getClass());
        }
    }

    private void handleWalletReload(WalletReloadEvent event) {
        System.out.println("Wallet reloaded: " + event);
        // Add your business logic here
    }

    private void handleTransferMoney(TransferMoneyEvent event) {
        System.out.println("Money transferred: " + event);
        // Add your business logic here
    }
}
