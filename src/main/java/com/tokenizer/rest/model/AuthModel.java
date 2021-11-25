package com.tokenizer.rest.model;

public class AuthModel {
    private String cardNumber;
    private String expirationDate;
    private String cardCVC;
    private String transactionId;

    public String getCardNumber() {
        return cardNumber;
    }

    public AuthModel setCardNumber(String cardNumber) {
        this.cardNumber = cardNumber;
        return this;
    }

    public String getExpirationDate() {
        return expirationDate;
    }

    public AuthModel setExpirationDate(String expirationDate) {
        this.expirationDate = expirationDate;
        return this;
    }

    public String getCardCVC() {
        return cardCVC;
    }

    public AuthModel setCardCVC(String cardCVC) {
        this.cardCVC = cardCVC;
        return this;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public AuthModel setTransactionId(String transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("AuthModel{");
        sb.append("cardNumber='").append(cardNumber).append('\'');
        sb.append(", expirationDate='").append(expirationDate).append('\'');
        sb.append(", cardCVC='").append(cardCVC).append('\'');
        sb.append(", transactionId='").append(transactionId).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
