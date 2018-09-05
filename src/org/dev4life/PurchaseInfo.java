package org.dev4life;

import java.util.Map;

public class PurchaseInfo {

    public class Product {
        public class Price {
            public String currency;
            public long amount;
        }
        public Price price;
        public String sku;
        public Map<String, String> props;
    }

    public String id;
    public String subscriberId;
    public long timestamp;
    public Product product;
}
