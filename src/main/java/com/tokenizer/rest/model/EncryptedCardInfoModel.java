package com.tokenizer.rest.model;

public class EncryptedCardInfoModel {
    private String transactionId;
    private String token;

    public EncryptedCardInfoModel(String transactionId, String token) {
        this.transactionId = transactionId;
        this.token = token;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public EncryptedCardInfoModel setTransactionId(String transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    public String getToken() {
        return token;
    }

    public EncryptedCardInfoModel setToken(String token) {
        this.token = token;
        return this;
    }
}
