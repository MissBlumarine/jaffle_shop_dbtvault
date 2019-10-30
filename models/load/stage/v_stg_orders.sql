{{- config(materialized='view', schema='STG', enabled=true, tags='stage') -}}

{%- set source_table = ref('raw_orders') -%}

{{ dbtvault.multi_hash([('CUSTOMERKEY', 'CUSTOMER_PK'),
(['CUSTOMERKEY', 'CUSTOMER_NATIONKEY'], 'CUSTOMER_NATION_PK'),
('CUSTOMER_NATIONKEY', 'NATION_PK'),
('CUSTOMER_REGIONKEY', 'REGION_PK'),
(['CUSTOMER_NATIONKEY', 'CUSTOMER_REGIONKEY'], 'NATION_REGION_PK'),
('ORDERKEY', 'ORDER_PK'),
(['CUSTOMERKEY', 'ORDERKEY'], 'ORDER_CUSTOMER_PK'),
('LINENUMBER', 'LINEITEM_PK'),
(['LINENUMBER', 'ORDERKEY'], 'LINEITEM_ORDER_PK'),
('PARTKEY', 'PART_PK'),
('SUPPLIERKEY', 'SUPPLIER_PK'),
(['PARTKEY', 'SUPPLIERKEY'], 'INVENTORY_PK'),
(['LINENUMBER', 'PARTKEY', 'SUPPLIERKEY'], 'INVENTORY_ALLOCATION_PK'),
(['CUSTOMERKEY', 'CUSTOMER_NAME', 'CUSTOMER_ADDRESS', 'CUSTOMER_PHONE', 'CUSTOMER_ACCBAL', 'CUSTOMER_MKTSEGMENT', 'CUSTOMER_COMMENT'], 'CUSTOMER_HASHDIFF', true),
(['CUSTOMER_NATIONKEY', 'CUSTOMER_NATION_COMMENT', 'CUSTOMER_NATION_NAME'], 'CUSTOMER_NATION_HASHDIFF', true),
(['CUSTOMER_REGIONKEY', 'CUSTOMER_REGION_COMMENT', 'CUSTOMER_REGION_NAME'], 'CUSTOMER_REGION_HASHDIFF', true),
(['ORDERKEY', 'LINENUMBER', 'COMMITDATE', 'DISCOUNT', 'EXTENDEDPRICE', 'LINESTATUS', 'LINE_COMMENT', 'QUANTITY', 'RECEIPTDATE', 'RETURNFLAG', 'SHIPDATE', 'SHIPINSTRUCT', 'SHIPMODE', 'TAX'], 'LINEITEM_HASHDIFF', true),
(['ORDERKEY', 'CLERK', 'ORDERDATE', 'ORDERPRIORITY', 'ORDERSTATUS', 'ORDER_COMMENT', 'SHIPPRIORITY', 'TOTALPRICE'], 'ORDER_HASHDIFF', true)]) -}},

{{ dbtvault.add_columns(source_table,
[('CUSTOMER_NAME', 'NAME'),
('CUSTOMER_ADDRESS', 'ADDRESS'),
('CUSTOMER_PHONE', 'PHONE'),
('CUSTOMER_ACCBAL', 'ACCBAL'),
('CUSTOMER_MKTSEGMENT', 'MKTSEGMENT'),
('CUSTOMER_COMMENT', 'COMMENT'),
('CUSTOMER_NATIONKEY', 'NATION_KEY'),
('CUSTOMER_NATION_NAME', 'NATION_NAME'),
('CUSTOMER_NATION_COMMENT', 'NATION_COMMENT'),
('CUSTOMER_REGIONKEY', 'REGION_KEY'),
('CUSTOMER_REGION_NAME', 'REGION_NAME'),
('CUSTOMER_REGION_COMMENT', 'REGION_COMMENT'),
('CUSTOMERKEY', 'CUSTOMER_KEY'),
('PARTKEY', 'PART_KEY'),
('SUPPLIERKEY', 'SUPPLIER_KEY'),
('LINENUMBER', 'LINEITEM_KEY'),
('ORDERKEY', 'ORDER_KEY'),
(var('date'), 'LOADDATE'),
('ORDERDATE', 'EFFECTIVE_FROM'),
('!TPCH-ORDERS', 'SOURCE')]) }}

{{ dbtvault.from(source_table) }}