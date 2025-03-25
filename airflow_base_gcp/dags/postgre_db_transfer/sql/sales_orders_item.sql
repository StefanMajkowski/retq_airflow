select
    soi.order_id                                     as "ORDER_ID"
  , soi.product_type                                 as "PRODUCT_TYPE"
  , soi.product_id                                   as "PRODUCT_ID"
  , soi.parent_item_id                               as "PARENT_ITEM_ID"
  , soi.item_id                                      as "ITEM_ID"
  , soi.name                                         as "NAME"
  , soi.qty_ordered                                  as "QTY_ORDERED"
  , soi.price                                        as "PRICE"
  , soi.add_to_cart_source                           as "ADD_TO_CART_SOURCE"
  , soi.discount_amount                              as "DISCOUNT_AMOUNT"
  , soi.tax_amount                                   as "TAX_AMOUNT"
  , soi.row_total_incl_tax                           as "ROW_TOTAL_INCL_TAX"
  , soi.original_price                               as "ORIGINAL_PRICE"
  , soi.tax_percent                                  as "TAX_PERCENT"
  , soi.discount_tax_compensation_amount             as "DISCOUNT_TAX_COMPENSATION_AMOUNT"
  , soi.sku                                          as "SKU"
  , soi.created_at                                   as "CREATED_AT"
  , soi.updated_at                                   as "UPDATED_AT"
  , '{{ data_interval_start }}'                      as "LOADED_AT"
from magento.sales_order_item soi
{% if not dag_run.conf.get('full_refresh') %}
    where soi.updated_at >= '{{ data_interval_start  - macros.timedelta(hours=3) }}'
{% endif %}