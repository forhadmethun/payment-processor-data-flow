package com.tokenizer.rest.request;

public class AuthRequest {
    private String cardNumber;
    private String expirationDate;
    private String cardCVC;

    public String getCardNumber() {
        return cardNumber;
    }

    public AuthRequest setCardNumber(String cardNumber) {
        this.cardNumber = cardNumber;
        return this;
    }

    public String getExpirationDate() {
        return expirationDate;
    }

    public AuthRequest setExpirationDate(String expirationDate) {
        this.expirationDate = expirationDate;
        return this;
    }

    public String getCardCVC() {
        return cardCVC;
    }

    public AuthRequest setCardCVC(String cardCVC) {
        this.cardCVC = cardCVC;
        return this;
    }
}
