create table if not exists bronze.magento.src_magento__sales_orders_item
(
    order_id                            string,
    product_type                        string,
    product_id                          int,
    parent_item_id                      int,
    item_id                             int,
    name                                string,
    qty_ordered                         float,
    price                               float,
    add_to_cart_source                  string,
    discount_amount                     float,
    tax_amount                          float,
    row_total_incl_tax                  float,
    original_price                      float,
    tax_percent                         float,
    discount_tax_compensation_amount    float,
    sku                                 string,
    created_at                          string,
    updated_at                          string,
    loaded_at                           string
);