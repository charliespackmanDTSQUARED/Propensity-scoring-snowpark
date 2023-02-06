-- Proc for creating a 60 day feature set for a given lag (t-1, t-2)
CREATE OR REPLACE PROCEDURE CREATE_FEATURE_STORE(LAG float)
    returns string
    language javascript
    strict

    as
    $$
    var sql_command = 
    
    `
    INSERT INTO CHARLIE_FEATURE_STORE
    -- Get label for t-1 <-> t-30 days
    SELECT 
        -- Date
        CURRENT_DATE - ` + LAG + ` - 29 as DATE,

        -- Household and product
        h.household_key,
        p.commodity_desc,

        -- Household only features
        features_hh.* EXCLUDE household_key,

        -- Household and product pairing features
        features_hh_prd.* EXCLUDE (household_key, commodity_desc),

        -- Label
        CASE WHEN t.household_key IS NOT NULL THEN 1 ELSE 0 END as PURCHASED

    FROM CHARLIE_HH_DEMOGRAPHIC h

    CROSS JOIN (SELECT DISTINCT commodity_desc FROM CHARLIE_PRODUCT) p

    -- Get purchased label
    LEFT JOIN 

        (SELECT DISTINCT
        household_key,
        COMMODITY_DESC
        FROM CHARLIE_TRANSACTION_DATA_ADJ
        WHERE (CURRENT_DATE() - ` + LAG + ` >= DATE) AND (CURRENT_DATE() - ` + LAG + ` - 29 <= DATE)) t

    ON h.household_key = t.household_key AND p.commodity_desc = t.commodity_desc


    -- Household features
    LEFT JOIN (

        -- Get features for t-30 <-> t-60 days houseghold
        SELECT 

        -- Summary stats
        t.*,

        -- Per-day ratios
        baskets/days as baskets_per_day,
        products/days as products_per_day,
        line_items/days as line_items_per_day, 
        amount_list/days as amount_list_per_day, 
        instore_discount/days as instore_discount_per_day, 
        campaign_coupon_discount/days as campaign_coupon_discount_per_day, 
        manuf_coupon_discount/days as manuf_coupon_discount_per_day, 
        total_coupon_discount/days as total_coupon_discount_per_day,
        amount_paid/days as amount_paid_per_day,
        days_with_instore_discount/days as days_with_instore_discount_per_days,
        days_with_campaign_coupon_discount/days as days_with_campaign_coupon_discount_per_days,
        days_with_manuf_coupon_discount/days as days_with_manuf_coupon_discount_per_days,
        days_with_total_coupon_discount/days as days_with_total_coupon_discount_per_days,

        -- Per-basket ratios
        products/baskets as products_per_basket,
        line_items/baskets as line_items_per_basket, 
        amount_list/baskets as amount_list_per_basket, 
        instore_discount/baskets as instore_discount_per_basket, 
        campaign_coupon_discount/baskets as campaign_coupon_discount_per_basket, 
        manuf_coupon_discount/baskets as manuf_coupon_discount_per_basket, 
        total_coupon_discount/baskets as total_coupon_discount_per_basket,
        amount_paid/baskets as amount_paid_per_basket,
        baskets_with_instore_discount/baskets as baskets_with_instore_discount_per_baskets,
        baskets_with_campaign_coupon_discount/baskets as baskets_with_campaign_coupon_discount_per_baskets,
        baskets_with_manuf_coupon_discount/baskets as baskets_with_manuf_coupon_discount_per_baskets,
        baskets_with_total_coupon_discount/baskets as baskets_with_total_coupon_discount_per_baskets,

        -- Per-product ratios
        line_items/products as line_items_per_product, 
        amount_list/products as amount_list_per_product, 
        instore_discount/products as instore_discount_per_product, 
        campaign_coupon_discount/products as campaign_coupon_discount_per_product, 
        manuf_coupon_discount/products as manuf_coupon_discount_per_product, 
        total_coupon_discount/products as total_coupon_discount_per_product,
        amount_paid/products as amount_paid_per_product,
        products_with_instore_discount/products as products_with_instore_discount_per_products,
        products_with_campaign_coupon_discount/products as products_with_campaign_coupon_discount_per_products,
        products_with_manuf_coupon_discount/products as products_with_manuf_coupon_discount_per_products,
        products_with_total_coupon_discount/products as products_with_total_coupon_discount_per_products,

        -- Per-line item ratios
        amount_list/line_items as amount_list_per_line_item, 
        instore_discount/line_items as instore_discount_per_line_item, 
        campaign_coupon_discount/line_items as campaign_coupon_discount_per_line_item, 
        manuf_coupon_discount/line_items as manuf_coupon_discount_per_line_item, 
        total_coupon_discount/line_items as total_coupon_discount_per_line_item,
        amount_paid/line_items as amount_paid_per_line_item,
        line_items_with_instore_discount/line_items as line_items_with_instore_discount_per_line_items,
        line_items_with_campaign_coupon_discount/line_items as line_items_with_campaign_coupon_discount_per_line_items,
        line_items_with_manuf_coupon_discount/line_items as line_items_with_manuf_coupon_discount_per_line_items,
        line_items_with_total_coupon_discount/line_items as line_items_with_total_coupon_discount_per_line_items

        FROM

        (SELECT
            HOUSEHOLD_KEY, 

            -- summary stats
            count(distinct(day)) as days,
            count(distinct(basket_id)) as baskets,
            count(product_id) as products,
            count(*) as line_items,
            sum(amount_list) as amount_list,
            sum(instore_discount) as instore_discount,
            sum(campaign_coupon_discount) as campaign_coupon_discount,
            sum(manuf_coupon_discount) as manuf_coupon_discount,
            sum(total_coupon_discount) as total_coupon_discount,
            sum(amount_paid) as amount_paid,

            -- unique days with activity
            COUNT(DISTINCT(case when instore_discount >0 then day else null end)) as days_with_instore_discount,
            COUNT(DISTINCT(case when campaign_coupon_discount >0 then day else null end)) as days_with_campaign_coupon_discount,
            COUNT(DISTINCT(case when manuf_coupon_discount >0 then day else null end)) as days_with_manuf_coupon_discount,
            COUNT(DISTINCT(case when total_coupon_discount >0 then day else null end)) as days_with_total_coupon_discount,

            -- unique baskets with activity
            Count(Distinct(case when instore_discount >0 then basket_id else null end)) as baskets_with_instore_discount,
            Count(Distinct(case when campaign_coupon_discount >0 then basket_id else null end)) as baskets_with_campaign_coupon_discount,
            Count(Distinct(case when manuf_coupon_discount >0 then basket_id else null end)) as baskets_with_manuf_coupon_discount,
            Count(Distinct(case when total_coupon_discount >0 then basket_id else null end)) as baskets_with_total_coupon_discount,          

            -- unique products with activity
            Count(Distinct(case when instore_discount >0 then product_id else null end)) as products_with_instore_discount,
            Count(Distinct(case when campaign_coupon_discount >0 then product_id else null end)) as products_with_campaign_coupon_discount,
            Count(Distinct(case when manuf_coupon_discount >0 then product_id else null end)) as products_with_manuf_coupon_discount,
            Count(Distinct(case when total_coupon_discount >0 then product_id else null end)) as products_with_total_coupon_discount,          

            -- unique line items with activity
            sum(case when instore_discount >0 then 1 else null end) as line_items_with_instore_discount,
            sum(case when campaign_coupon_discount >0 then 1 else null end) as line_items_with_campaign_coupon_discount,
            sum(case when manuf_coupon_discount >0 then 1 else null end) as line_items_with_manuf_coupon_discount,
            sum(case when total_coupon_discount >0 then 1 else null end) as line_items_with_total_coupon_discount          

        FROM CHARLIE_TRANSACTION_DATA_ADJ

        WHERE (CURRENT_DATE() - ` + LAG + ` - 29 >= DATE) AND (CURRENT_DATE() - ` + LAG + ` - 59 <= DATE)

        Group By HOUSEHOLD_KEY) t) features_hh

    ON h.household_key = features_hh.household_key

    LEFT JOIN (
        -- Get features for t-30 <-> t-60 days houseghold and commodity
        SELECT 

        -- Summary stats
        t.*,

        -- Per-day ratios
        prd_baskets/prd_days as prd_baskets_per_day,
        prd_products/prd_days as prd_products_per_day,
        prd_line_items/prd_days as prd_line_items_per_day, 
        prd_amount_list/prd_days as prd_amount_list_per_day, 
        prd_instore_discount/prd_days as prd_instore_discount_per_day, 
        prd_campaign_coupon_discount/prd_days as prd_campaign_coupon_discount_per_day, 
        prd_manuf_coupon_discount/prd_days as prd_manuf_coupon_discount_per_day, 
        prd_total_coupon_discount/prd_days as prd_total_coupon_discount_per_day,
        prd_amount_paid/prd_days as prd_amount_paid_per_day,
        prd_days_with_instore_discount/prd_days as prd_days_with_instore_discount_per_days,
        prd_days_with_campaign_coupon_discount/prd_days as prd_days_with_campaign_coupon_discount_per_days,
        prd_days_with_manuf_coupon_discount/prd_days as prd_days_with_manuf_coupon_discount_per_days,
        prd_days_with_total_coupon_discount/prd_days as prd_days_with_total_coupon_discount_per_days,

        -- Per-basket ratios
        prd_products/prd_baskets as prd_products_per_basket,
        prd_line_items/prd_baskets as prd_line_items_per_basket, 
        prd_amount_list/prd_baskets as prd_amount_list_per_basket, 
        prd_instore_discount/prd_baskets as prd_instore_discount_per_basket, 
        prd_campaign_coupon_discount/prd_baskets as prd_campaign_coupon_discount_per_basket, 
        prd_manuf_coupon_discount/prd_baskets as prd_manuf_coupon_discount_per_basket, 
        prd_total_coupon_discount/prd_baskets as prd_total_coupon_discount_per_basket,
        prd_amount_paid/prd_baskets as prd_amount_paid_per_basket,
        prd_baskets_with_instore_discount/prd_baskets as prd_baskets_with_instore_discount_per_baskets,
        prd_baskets_with_campaign_coupon_discount/prd_baskets as prd_baskets_with_campaign_coupon_discount_per_baskets,
        prd_baskets_with_manuf_coupon_discount/prd_baskets as prd_baskets_with_manuf_coupon_discount_per_baskets,
        prd_baskets_with_total_coupon_discount/prd_baskets as prd_baskets_with_total_coupon_discount_per_baskets,

        -- Per-product ratios
        prd_line_items/prd_products as prd_line_items_per_product, 
        prd_amount_list/prd_products as prd_amount_list_per_product, 
        prd_instore_discount/prd_products as prd_instore_discount_per_product, 
        prd_campaign_coupon_discount/prd_products as prd_campaign_coupon_discount_per_product, 
        prd_manuf_coupon_discount/prd_products as prd_manuf_coupon_discount_per_product, 
        prd_total_coupon_discount/prd_products as prd_total_coupon_discount_per_product,
        prd_amount_paid/prd_products as prd_amount_paid_per_product,
        prd_products_with_instore_discount/prd_products as prd_products_with_instore_discount_per_products,
        prd_products_with_campaign_coupon_discount/prd_products as prd_products_with_campaign_coupon_discount_per_products,
        prd_products_with_manuf_coupon_discount/prd_products as prd_products_with_manuf_coupon_discount_per_products,
        prd_products_with_total_coupon_discount/prd_products as prd_products_with_total_coupon_discount_per_products,

        -- Per-line item ratios
        prd_amount_list/prd_line_items as prd_amount_list_per_line_item, 
        prd_instore_discount/prd_line_items as prd_instore_discount_per_line_item, 
        prd_campaign_coupon_discount/prd_line_items as prd_campaign_coupon_discount_per_line_item, 
        prd_manuf_coupon_discount/prd_line_items as prd_manuf_coupon_discount_per_line_item, 
        prd_total_coupon_discount/prd_line_items as prd_total_coupon_discount_per_line_item,
        prd_amount_paid/prd_line_items as prd_amount_paid_per_line_item,
        prd_line_items_with_instore_discount/prd_line_items as prd_line_items_with_instore_discount_per_line_items,
        prd_line_items_with_campaign_coupon_discount/prd_line_items as prd_line_items_with_campaign_coupon_discount_per_line_items,
        prd_line_items_with_manuf_coupon_discount/prd_line_items as prd_line_items_with_manuf_coupon_discount_per_line_items,
        prd_line_items_with_total_coupon_discount/prd_line_items as prd_line_items_with_total_coupon_discount_per_line_items

        FROM

        (SELECT
            HOUSEHOLD_KEY,
            COMMODITY_DESC, 

            -- summary stats
            count(distinct(day)) as prd_days,
            count(distinct(basket_id)) as prd_baskets,
            count(product_id) as prd_products,
            count(*) as prd_line_items,
            sum(amount_list) as prd_amount_list,
            sum(instore_discount) as prd_instore_discount,
            sum(campaign_coupon_discount) as prd_campaign_coupon_discount,
            sum(manuf_coupon_discount) as prd_manuf_coupon_discount,
            sum(total_coupon_discount) as prd_total_coupon_discount,
            sum(amount_paid) as prd_amount_paid,

            -- unique days with activity
            COUNT(DISTINCT(case when instore_discount >0 then day else null end)) as prd_days_with_instore_discount,
            COUNT(DISTINCT(case when campaign_coupon_discount >0 then day else null end)) as prd_days_with_campaign_coupon_discount,
            COUNT(DISTINCT(case when manuf_coupon_discount >0 then day else null end)) as prd_days_with_manuf_coupon_discount,
            COUNT(DISTINCT(case when total_coupon_discount >0 then day else null end)) as prd_days_with_total_coupon_discount,

            -- unique baskets with activity
            Count(Distinct(case when instore_discount >0 then basket_id else null end)) as prd_baskets_with_instore_discount,
            Count(Distinct(case when campaign_coupon_discount >0 then basket_id else null end)) as prd_baskets_with_campaign_coupon_discount,
            Count(Distinct(case when manuf_coupon_discount >0 then basket_id else null end)) as prd_baskets_with_manuf_coupon_discount,
            Count(Distinct(case when total_coupon_discount >0 then basket_id else null end)) as prd_baskets_with_total_coupon_discount,          

            -- unique products with activity
            Count(Distinct(case when instore_discount >0 then product_id else null end)) as prd_products_with_instore_discount,
            Count(Distinct(case when campaign_coupon_discount >0 then product_id else null end)) as prd_products_with_campaign_coupon_discount,
            Count(Distinct(case when manuf_coupon_discount >0 then product_id else null end)) as prd_products_with_manuf_coupon_discount,
            Count(Distinct(case when total_coupon_discount >0 then product_id else null end)) as prd_products_with_total_coupon_discount,          

            -- unique line items with activity
            sum(case when instore_discount >0 then 1 else null end) as prd_line_items_with_instore_discount,
            sum(case when campaign_coupon_discount >0 then 1 else null end) as prd_line_items_with_campaign_coupon_discount,
            sum(case when manuf_coupon_discount >0 then 1 else null end) as prd_line_items_with_manuf_coupon_discount,
            sum(case when total_coupon_discount >0 then 1 else null end) as prd_line_items_with_total_coupon_discount          

        FROM CHARLIE_TRANSACTION_DATA_ADJ

        WHERE (CURRENT_DATE() - ` + LAG + ` - 29 >= DATE) AND (CURRENT_DATE() - ` + LAG + ` - 59 <= DATE)

        Group By HOUSEHOLD_KEY, COMMODITY_DESC) t) features_hh_prd

    ON h.household_key = features_hh_prd.household_key AND p.commodity_desc = features_hh_prd.commodity_desc

    WHERE p.commodity_desc NOT IN (' ', '(CORP USE ONLY)', 'UNKNOWN')

    ORDER BY h.household_key, p.commodity_desc
     
     `;
     
     
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "Succeeded.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    $$
    ;
    
    
CREATE OR REPLACE TABLE CHARLIE_FEATURE_STORE 
    (DATE DATE,
    HOUSEHOLD_KEY NUMBER,
    COMMODITY_DESC VARCHAR,
    DAYS NUMBER,
    BASKETS NUMBER,
    PRODUCTS NUMBER,
    LINE_ITEMS NUMBER,
    AMOUNT_LIST NUMBER,
    INSTORE_DISCOUNT NUMBER,
    CAMPAIGN_COUPON_DISCOUNT NUMBER,
    MANUF_COUPON_DISCOUNT NUMBER,
    TOTAL_COUPON_DISCOUNT NUMBER,
    AMOUNT_PAID NUMBER,
    DAYS_WITH_INSTORE_DISCOUNT NUMBER,
    DAYS_WITH_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    DAYS_WITH_MANUF_COUPON_DISCOUNT NUMBER,
    DAYS_WITH_TOTAL_COUPON_DISCOUNT NUMBER,
    BASKETS_WITH_INSTORE_DISCOUNT NUMBER,
    BASKETS_WITH_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    BASKETS_WITH_MANUF_COUPON_DISCOUNT NUMBER,
    BASKETS_WITH_TOTAL_COUPON_DISCOUNT NUMBER,
    PRODUCTS_WITH_INSTORE_DISCOUNT NUMBER,
    PRODUCTS_WITH_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    PRODUCTS_WITH_MANUF_COUPON_DISCOUNT NUMBER,
    PRODUCTS_WITH_TOTAL_COUPON_DISCOUNT NUMBER,
    LINE_ITEMS_WITH_INSTORE_DISCOUNT NUMBER,
    LINE_ITEMS_WITH_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    LINE_ITEMS_WITH_MANUF_COUPON_DISCOUNT NUMBER,
    LINE_ITEMS_WITH_TOTAL_COUPON_DISCOUNT NUMBER,
    BASKETS_PER_DAY NUMBER,
    PRODUCTS_PER_DAY NUMBER,
    LINE_ITEMS_PER_DAY NUMBER,
    AMOUNT_LIST_PER_DAY NUMBER,
    INSTORE_DISCOUNT_PER_DAY NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_PER_DAY NUMBER,
    MANUF_COUPON_DISCOUNT_PER_DAY NUMBER,
    TOTAL_COUPON_DISCOUNT_PER_DAY NUMBER,
    AMOUNT_PAID_PER_DAY NUMBER,
    DAYS_WITH_INSTORE_DISCOUNT_PER_DAYS NUMBER,
    DAYS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_DAYS NUMBER,
    DAYS_WITH_MANUF_COUPON_DISCOUNT_PER_DAYS NUMBER,
    DAYS_WITH_TOTAL_COUPON_DISCOUNT_PER_DAYS NUMBER,
    PRODUCTS_PER_BASKET NUMBER,
    LINE_ITEMS_PER_BASKET NUMBER,
    AMOUNT_LIST_PER_BASKET NUMBER,
    INSTORE_DISCOUNT_PER_BASKET NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_PER_BASKET NUMBER,
    MANUF_COUPON_DISCOUNT_PER_BASKET NUMBER,
    TOTAL_COUPON_DISCOUNT_PER_BASKET NUMBER,
    AMOUNT_PAID_PER_BASKET NUMBER,
    BASKETS_WITH_INSTORE_DISCOUNT_PER_BASKETS NUMBER,
    BASKETS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_BASKETS NUMBER,
    BASKETS_WITH_MANUF_COUPON_DISCOUNT_PER_BASKETS NUMBER,
    BASKETS_WITH_TOTAL_COUPON_DISCOUNT_PER_BASKETS NUMBER,
    LINE_ITEMS_PER_PRODUCT NUMBER,
    AMOUNT_LIST_PER_PRODUCT NUMBER,
    INSTORE_DISCOUNT_PER_PRODUCT NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_PER_PRODUCT NUMBER,
    MANUF_COUPON_DISCOUNT_PER_PRODUCT NUMBER,
    TOTAL_COUPON_DISCOUNT_PER_PRODUCT NUMBER,
    AMOUNT_PAID_PER_PRODUCT NUMBER,
    PRODUCTS_WITH_INSTORE_DISCOUNT_PER_PRODUCTS NUMBER,
    PRODUCTS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_PRODUCTS NUMBER,
    PRODUCTS_WITH_MANUF_COUPON_DISCOUNT_PER_PRODUCTS NUMBER,
    PRODUCTS_WITH_TOTAL_COUPON_DISCOUNT_PER_PRODUCTS NUMBER,
    AMOUNT_LIST_PER_LINE_ITEM NUMBER,
    INSTORE_DISCOUNT_PER_LINE_ITEM NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_PER_LINE_ITEM NUMBER,
    MANUF_COUPON_DISCOUNT_PER_LINE_ITEM NUMBER,
    TOTAL_COUPON_DISCOUNT_PER_LINE_ITEM NUMBER,
    AMOUNT_PAID_PER_LINE_ITEM NUMBER,
    LINE_ITEMS_WITH_INSTORE_DISCOUNT_PER_LINE_ITEMS NUMBER,
    LINE_ITEMS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_LINE_ITEMS NUMBER,
    LINE_ITEMS_WITH_MANUF_COUPON_DISCOUNT_PER_LINE_ITEMS NUMBER,
    LINE_ITEMS_WITH_TOTAL_COUPON_DISCOUNT_PER_LINE_ITEMS NUMBER,
    PRD_DAYS NUMBER,
    PRD_BASKETS NUMBER,
    PRD_PRODUCTS NUMBER,
    PRD_LINE_ITEMS NUMBER,
    PRD_AMOUNT_LIST NUMBER,
    PRD_INSTORE_DISCOUNT NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    PRD_MANUF_COUPON_DISCOUNT NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT NUMBER,
    PRD_AMOUNT_PAID NUMBER,
    PRD_DAYS_WITH_INSTORE_DISCOUNT NUMBER,
    PRD_DAYS_WITH_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    PRD_DAYS_WITH_MANUF_COUPON_DISCOUNT NUMBER,
    PRD_DAYS_WITH_TOTAL_COUPON_DISCOUNT NUMBER,
    PRD_BASKETS_WITH_INSTORE_DISCOUNT NUMBER,
    PRD_BASKETS_WITH_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    PRD_BASKETS_WITH_MANUF_COUPON_DISCOUNT NUMBER,
    PRD_BASKETS_WITH_TOTAL_COUPON_DISCOUNT NUMBER,
    PRD_PRODUCTS_WITH_INSTORE_DISCOUNT NUMBER,
    PRD_PRODUCTS_WITH_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    PRD_PRODUCTS_WITH_MANUF_COUPON_DISCOUNT NUMBER,
    PRD_PRODUCTS_WITH_TOTAL_COUPON_DISCOUNT NUMBER,
    PRD_LINE_ITEMS_WITH_INSTORE_DISCOUNT NUMBER,
    PRD_LINE_ITEMS_WITH_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    PRD_LINE_ITEMS_WITH_MANUF_COUPON_DISCOUNT NUMBER,
    PRD_LINE_ITEMS_WITH_TOTAL_COUPON_DISCOUNT NUMBER,
    PRD_BASKETS_PER_DAY NUMBER,
    PRD_PRODUCTS_PER_DAY NUMBER,
    PRD_LINE_ITEMS_PER_DAY NUMBER,
    PRD_AMOUNT_LIST_PER_DAY NUMBER,
    PRD_INSTORE_DISCOUNT_PER_DAY NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_PER_DAY NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_PER_DAY NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_PER_DAY NUMBER,
    PRD_AMOUNT_PAID_PER_DAY NUMBER,
    PRD_DAYS_WITH_INSTORE_DISCOUNT_PER_DAYS NUMBER,
    PRD_DAYS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_DAYS NUMBER,
    PRD_DAYS_WITH_MANUF_COUPON_DISCOUNT_PER_DAYS NUMBER,
    PRD_DAYS_WITH_TOTAL_COUPON_DISCOUNT_PER_DAYS NUMBER,
    PRD_PRODUCTS_PER_BASKET NUMBER,
    PRD_LINE_ITEMS_PER_BASKET NUMBER,
    PRD_AMOUNT_LIST_PER_BASKET NUMBER,
    PRD_INSTORE_DISCOUNT_PER_BASKET NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_PER_BASKET NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_PER_BASKET NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_PER_BASKET NUMBER,
    PRD_AMOUNT_PAID_PER_BASKET NUMBER,
    PRD_BASKETS_WITH_INSTORE_DISCOUNT_PER_BASKETS NUMBER,
    PRD_BASKETS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_BASKETS NUMBER,
    PRD_BASKETS_WITH_MANUF_COUPON_DISCOUNT_PER_BASKETS NUMBER,
    PRD_BASKETS_WITH_TOTAL_COUPON_DISCOUNT_PER_BASKETS NUMBER,
    PRD_LINE_ITEMS_PER_PRODUCT NUMBER,
    PRD_AMOUNT_LIST_PER_PRODUCT NUMBER,
    PRD_INSTORE_DISCOUNT_PER_PRODUCT NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_PER_PRODUCT NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_PER_PRODUCT NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_PER_PRODUCT NUMBER,
    PRD_AMOUNT_PAID_PER_PRODUCT NUMBER,
    PRD_PRODUCTS_WITH_INSTORE_DISCOUNT_PER_PRODUCTS NUMBER,
    PRD_PRODUCTS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_PRODUCTS NUMBER,
    PRD_PRODUCTS_WITH_MANUF_COUPON_DISCOUNT_PER_PRODUCTS NUMBER,
    PRD_PRODUCTS_WITH_TOTAL_COUPON_DISCOUNT_PER_PRODUCTS NUMBER,
    PRD_AMOUNT_LIST_PER_LINE_ITEM NUMBER,
    PRD_INSTORE_DISCOUNT_PER_LINE_ITEM NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_PER_LINE_ITEM NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_PER_LINE_ITEM NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_PER_LINE_ITEM NUMBER,
    PRD_AMOUNT_PAID_PER_LINE_ITEM NUMBER,
    PRD_LINE_ITEMS_WITH_INSTORE_DISCOUNT_PER_LINE_ITEMS NUMBER,
    PRD_LINE_ITEMS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_LINE_ITEMS NUMBER,
    PRD_LINE_ITEMS_WITH_MANUF_COUPON_DISCOUNT_PER_LINE_ITEMS NUMBER,
    PRD_LINE_ITEMS_WITH_TOTAL_COUPON_DISCOUNT_PER_LINE_ITEMS NUMBER,
    PURCHASED NUMBER);



-- Proc for creating a 30 fay feature set for a given lag (t-1, t-2)
CREATE OR REPLACE PROCEDURE CREATE_FEATURES(LAG float)
    returns string
    language javascript
    strict

    as
    $$
    var sql_command = 
    
    `
    INSERT INTO CHARLIE_FEATURES
    SELECT 
        -- Date
        CURRENT_DATE - ` + LAG + ` as DATE,

        -- Household and product
        h.household_key,
        p.commodity_desc,

        -- Household only features
        features_hh.* EXCLUDE household_key,

        -- Household and product pairing features
        features_hh_prd.* EXCLUDE (household_key, commodity_desc),

        -- Label
        NULL AS PURCHASED

    FROM CHARLIE_HH_DEMOGRAPHIC h

    CROSS JOIN (SELECT DISTINCT commodity_desc FROM CHARLIE_PRODUCT) p

    -- Household features
    LEFT JOIN (

        -- Get features for t-30 <-> t-60 days houseghold
        SELECT 

        -- Summary stats
        t.*,

        -- Per-day ratios
        baskets/days as baskets_per_day,
        products/days as products_per_day,
        line_items/days as line_items_per_day, 
        amount_list/days as amount_list_per_day, 
        instore_discount/days as instore_discount_per_day, 
        campaign_coupon_discount/days as campaign_coupon_discount_per_day, 
        manuf_coupon_discount/days as manuf_coupon_discount_per_day, 
        total_coupon_discount/days as total_coupon_discount_per_day,
        amount_paid/days as amount_paid_per_day,
        days_with_instore_discount/days as days_with_instore_discount_per_days,
        days_with_campaign_coupon_discount/days as days_with_campaign_coupon_discount_per_days,
        days_with_manuf_coupon_discount/days as days_with_manuf_coupon_discount_per_days,
        days_with_total_coupon_discount/days as days_with_total_coupon_discount_per_days,

        -- Per-basket ratios
        products/baskets as products_per_basket,
        line_items/baskets as line_items_per_basket, 
        amount_list/baskets as amount_list_per_basket, 
        instore_discount/baskets as instore_discount_per_basket, 
        campaign_coupon_discount/baskets as campaign_coupon_discount_per_basket, 
        manuf_coupon_discount/baskets as manuf_coupon_discount_per_basket, 
        total_coupon_discount/baskets as total_coupon_discount_per_basket,
        amount_paid/baskets as amount_paid_per_basket,
        baskets_with_instore_discount/baskets as baskets_with_instore_discount_per_baskets,
        baskets_with_campaign_coupon_discount/baskets as baskets_with_campaign_coupon_discount_per_baskets,
        baskets_with_manuf_coupon_discount/baskets as baskets_with_manuf_coupon_discount_per_baskets,
        baskets_with_total_coupon_discount/baskets as baskets_with_total_coupon_discount_per_baskets,

        -- Per-product ratios
        line_items/products as line_items_per_product, 
        amount_list/products as amount_list_per_product, 
        instore_discount/products as instore_discount_per_product, 
        campaign_coupon_discount/products as campaign_coupon_discount_per_product, 
        manuf_coupon_discount/products as manuf_coupon_discount_per_product, 
        total_coupon_discount/products as total_coupon_discount_per_product,
        amount_paid/products as amount_paid_per_product,
        products_with_instore_discount/products as products_with_instore_discount_per_products,
        products_with_campaign_coupon_discount/products as products_with_campaign_coupon_discount_per_products,
        products_with_manuf_coupon_discount/products as products_with_manuf_coupon_discount_per_products,
        products_with_total_coupon_discount/products as products_with_total_coupon_discount_per_products,

        -- Per-line item ratios
        amount_list/line_items as amount_list_per_line_item, 
        instore_discount/line_items as instore_discount_per_line_item, 
        campaign_coupon_discount/line_items as campaign_coupon_discount_per_line_item, 
        manuf_coupon_discount/line_items as manuf_coupon_discount_per_line_item, 
        total_coupon_discount/line_items as total_coupon_discount_per_line_item,
        amount_paid/line_items as amount_paid_per_line_item,
        line_items_with_instore_discount/line_items as line_items_with_instore_discount_per_line_items,
        line_items_with_campaign_coupon_discount/line_items as line_items_with_campaign_coupon_discount_per_line_items,
        line_items_with_manuf_coupon_discount/line_items as line_items_with_manuf_coupon_discount_per_line_items,
        line_items_with_total_coupon_discount/line_items as line_items_with_total_coupon_discount_per_line_items

        FROM

        (SELECT
            HOUSEHOLD_KEY, 

            -- summary stats
            count(distinct(day)) as days,
            count(distinct(basket_id)) as baskets,
            count(product_id) as products,
            count(*) as line_items,
            sum(amount_list) as amount_list,
            sum(instore_discount) as instore_discount,
            sum(campaign_coupon_discount) as campaign_coupon_discount,
            sum(manuf_coupon_discount) as manuf_coupon_discount,
            sum(total_coupon_discount) as total_coupon_discount,
            sum(amount_paid) as amount_paid,

            -- unique days with activity
            COUNT(DISTINCT(case when instore_discount >0 then day else null end)) as days_with_instore_discount,
            COUNT(DISTINCT(case when campaign_coupon_discount >0 then day else null end)) as days_with_campaign_coupon_discount,
            COUNT(DISTINCT(case when manuf_coupon_discount >0 then day else null end)) as days_with_manuf_coupon_discount,
            COUNT(DISTINCT(case when total_coupon_discount >0 then day else null end)) as days_with_total_coupon_discount,

            -- unique baskets with activity
            Count(Distinct(case when instore_discount >0 then basket_id else null end)) as baskets_with_instore_discount,
            Count(Distinct(case when campaign_coupon_discount >0 then basket_id else null end)) as baskets_with_campaign_coupon_discount,
            Count(Distinct(case when manuf_coupon_discount >0 then basket_id else null end)) as baskets_with_manuf_coupon_discount,
            Count(Distinct(case when total_coupon_discount >0 then basket_id else null end)) as baskets_with_total_coupon_discount,          

            -- unique products with activity
            Count(Distinct(case when instore_discount >0 then product_id else null end)) as products_with_instore_discount,
            Count(Distinct(case when campaign_coupon_discount >0 then product_id else null end)) as products_with_campaign_coupon_discount,
            Count(Distinct(case when manuf_coupon_discount >0 then product_id else null end)) as products_with_manuf_coupon_discount,
            Count(Distinct(case when total_coupon_discount >0 then product_id else null end)) as products_with_total_coupon_discount,          

            -- unique line items with activity
            sum(case when instore_discount >0 then 1 else null end) as line_items_with_instore_discount,
            sum(case when campaign_coupon_discount >0 then 1 else null end) as line_items_with_campaign_coupon_discount,
            sum(case when manuf_coupon_discount >0 then 1 else null end) as line_items_with_manuf_coupon_discount,
            sum(case when total_coupon_discount >0 then 1 else null end) as line_items_with_total_coupon_discount          

        FROM CHARLIE_TRANSACTION_DATA_ADJ

        WHERE (CURRENT_DATE() - ` + LAG + ` >= DATE) AND (CURRENT_DATE() - ` + LAG + ` - 29 <= DATE)

        Group By HOUSEHOLD_KEY) t) features_hh

    ON h.household_key = features_hh.household_key

    LEFT JOIN (
        -- Get features for t-30 <-> t-60 days houseghold and commodity
        SELECT 

        -- Summary stats
        t.*,

        -- Per-day ratios
        prd_baskets/prd_days as prd_baskets_per_day,
        prd_products/prd_days as prd_products_per_day,
        prd_line_items/prd_days as prd_line_items_per_day, 
        prd_amount_list/prd_days as prd_amount_list_per_day, 
        prd_instore_discount/prd_days as prd_instore_discount_per_day, 
        prd_campaign_coupon_discount/prd_days as prd_campaign_coupon_discount_per_day, 
        prd_manuf_coupon_discount/prd_days as prd_manuf_coupon_discount_per_day, 
        prd_total_coupon_discount/prd_days as prd_total_coupon_discount_per_day,
        prd_amount_paid/prd_days as prd_amount_paid_per_day,
        prd_days_with_instore_discount/prd_days as prd_days_with_instore_discount_per_days,
        prd_days_with_campaign_coupon_discount/prd_days as prd_days_with_campaign_coupon_discount_per_days,
        prd_days_with_manuf_coupon_discount/prd_days as prd_days_with_manuf_coupon_discount_per_days,
        prd_days_with_total_coupon_discount/prd_days as prd_days_with_total_coupon_discount_per_days,

        -- Per-basket ratios
        prd_products/prd_baskets as prd_products_per_basket,
        prd_line_items/prd_baskets as prd_line_items_per_basket, 
        prd_amount_list/prd_baskets as prd_amount_list_per_basket, 
        prd_instore_discount/prd_baskets as prd_instore_discount_per_basket, 
        prd_campaign_coupon_discount/prd_baskets as prd_campaign_coupon_discount_per_basket, 
        prd_manuf_coupon_discount/prd_baskets as prd_manuf_coupon_discount_per_basket, 
        prd_total_coupon_discount/prd_baskets as prd_total_coupon_discount_per_basket,
        prd_amount_paid/prd_baskets as prd_amount_paid_per_basket,
        prd_baskets_with_instore_discount/prd_baskets as prd_baskets_with_instore_discount_per_baskets,
        prd_baskets_with_campaign_coupon_discount/prd_baskets as prd_baskets_with_campaign_coupon_discount_per_baskets,
        prd_baskets_with_manuf_coupon_discount/prd_baskets as prd_baskets_with_manuf_coupon_discount_per_baskets,
        prd_baskets_with_total_coupon_discount/prd_baskets as prd_baskets_with_total_coupon_discount_per_baskets,

        -- Per-product ratios
        prd_line_items/prd_products as prd_line_items_per_product, 
        prd_amount_list/prd_products as prd_amount_list_per_product, 
        prd_instore_discount/prd_products as prd_instore_discount_per_product, 
        prd_campaign_coupon_discount/prd_products as prd_campaign_coupon_discount_per_product, 
        prd_manuf_coupon_discount/prd_products as prd_manuf_coupon_discount_per_product, 
        prd_total_coupon_discount/prd_products as prd_total_coupon_discount_per_product,
        prd_amount_paid/prd_products as prd_amount_paid_per_product,
        prd_products_with_instore_discount/prd_products as prd_products_with_instore_discount_per_products,
        prd_products_with_campaign_coupon_discount/prd_products as prd_products_with_campaign_coupon_discount_per_products,
        prd_products_with_manuf_coupon_discount/prd_products as prd_products_with_manuf_coupon_discount_per_products,
        prd_products_with_total_coupon_discount/prd_products as prd_products_with_total_coupon_discount_per_products,

        -- Per-line item ratios
        prd_amount_list/prd_line_items as prd_amount_list_per_line_item, 
        prd_instore_discount/prd_line_items as prd_instore_discount_per_line_item, 
        prd_campaign_coupon_discount/prd_line_items as prd_campaign_coupon_discount_per_line_item, 
        prd_manuf_coupon_discount/prd_line_items as prd_manuf_coupon_discount_per_line_item, 
        prd_total_coupon_discount/prd_line_items as prd_total_coupon_discount_per_line_item,
        prd_amount_paid/prd_line_items as prd_amount_paid_per_line_item,
        prd_line_items_with_instore_discount/prd_line_items as prd_line_items_with_instore_discount_per_line_items,
        prd_line_items_with_campaign_coupon_discount/prd_line_items as prd_line_items_with_campaign_coupon_discount_per_line_items,
        prd_line_items_with_manuf_coupon_discount/prd_line_items as prd_line_items_with_manuf_coupon_discount_per_line_items,
        prd_line_items_with_total_coupon_discount/prd_line_items as prd_line_items_with_total_coupon_discount_per_line_items

        FROM

        (SELECT
            HOUSEHOLD_KEY,
            COMMODITY_DESC, 

            -- summary stats
            count(distinct(day)) as prd_days,
            count(distinct(basket_id)) as prd_baskets,
            count(product_id) as prd_products,
            count(*) as prd_line_items,
            sum(amount_list) as prd_amount_list,
            sum(instore_discount) as prd_instore_discount,
            sum(campaign_coupon_discount) as prd_campaign_coupon_discount,
            sum(manuf_coupon_discount) as prd_manuf_coupon_discount,
            sum(total_coupon_discount) as prd_total_coupon_discount,
            sum(amount_paid) as prd_amount_paid,

            -- unique days with activity
            COUNT(DISTINCT(case when instore_discount >0 then day else null end)) as prd_days_with_instore_discount,
            COUNT(DISTINCT(case when campaign_coupon_discount >0 then day else null end)) as prd_days_with_campaign_coupon_discount,
            COUNT(DISTINCT(case when manuf_coupon_discount >0 then day else null end)) as prd_days_with_manuf_coupon_discount,
            COUNT(DISTINCT(case when total_coupon_discount >0 then day else null end)) as prd_days_with_total_coupon_discount,

            -- unique baskets with activity
            Count(Distinct(case when instore_discount >0 then basket_id else null end)) as prd_baskets_with_instore_discount,
            Count(Distinct(case when campaign_coupon_discount >0 then basket_id else null end)) as prd_baskets_with_campaign_coupon_discount,
            Count(Distinct(case when manuf_coupon_discount >0 then basket_id else null end)) as prd_baskets_with_manuf_coupon_discount,
            Count(Distinct(case when total_coupon_discount >0 then basket_id else null end)) as prd_baskets_with_total_coupon_discount,          

            -- unique products with activity
            Count(Distinct(case when instore_discount >0 then product_id else null end)) as prd_products_with_instore_discount,
            Count(Distinct(case when campaign_coupon_discount >0 then product_id else null end)) as prd_products_with_campaign_coupon_discount,
            Count(Distinct(case when manuf_coupon_discount >0 then product_id else null end)) as prd_products_with_manuf_coupon_discount,
            Count(Distinct(case when total_coupon_discount >0 then product_id else null end)) as prd_products_with_total_coupon_discount,          

            -- unique line items with activity
            sum(case when instore_discount >0 then 1 else null end) as prd_line_items_with_instore_discount,
            sum(case when campaign_coupon_discount >0 then 1 else null end) as prd_line_items_with_campaign_coupon_discount,
            sum(case when manuf_coupon_discount >0 then 1 else null end) as prd_line_items_with_manuf_coupon_discount,
            sum(case when total_coupon_discount >0 then 1 else null end) as prd_line_items_with_total_coupon_discount          

        FROM CHARLIE_TRANSACTION_DATA_ADJ

        WHERE (CURRENT_DATE() - ` + LAG + ` >= DATE) AND (CURRENT_DATE() - ` + LAG + ` - 29 <= DATE)

        Group By HOUSEHOLD_KEY, COMMODITY_DESC) t) features_hh_prd

    ON h.household_key = features_hh_prd.household_key AND p.commodity_desc = features_hh_prd.commodity_desc

    WHERE p.commodity_desc NOT IN (' ', '(CORP USE ONLY)', 'UNKNOWN')

    ORDER BY h.household_key, p.commodity_desc

     `;
     
     
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "Succeeded.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    $$
    ;
    
    
CREATE OR REPLACE TABLE CHARLIE_FEATURES 
    (DATE DATE,
    HOUSEHOLD_KEY NUMBER,
    COMMODITY_DESC VARCHAR,
    DAYS NUMBER,
    BASKETS NUMBER,
    PRODUCTS NUMBER,
    LINE_ITEMS NUMBER,
    AMOUNT_LIST NUMBER,
    INSTORE_DISCOUNT NUMBER,
    CAMPAIGN_COUPON_DISCOUNT NUMBER,
    MANUF_COUPON_DISCOUNT NUMBER,
    TOTAL_COUPON_DISCOUNT NUMBER,
    AMOUNT_PAID NUMBER,
    DAYS_WITH_INSTORE_DISCOUNT NUMBER,
    DAYS_WITH_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    DAYS_WITH_MANUF_COUPON_DISCOUNT NUMBER,
    DAYS_WITH_TOTAL_COUPON_DISCOUNT NUMBER,
    BASKETS_WITH_INSTORE_DISCOUNT NUMBER,
    BASKETS_WITH_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    BASKETS_WITH_MANUF_COUPON_DISCOUNT NUMBER,
    BASKETS_WITH_TOTAL_COUPON_DISCOUNT NUMBER,
    PRODUCTS_WITH_INSTORE_DISCOUNT NUMBER,
    PRODUCTS_WITH_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    PRODUCTS_WITH_MANUF_COUPON_DISCOUNT NUMBER,
    PRODUCTS_WITH_TOTAL_COUPON_DISCOUNT NUMBER,
    LINE_ITEMS_WITH_INSTORE_DISCOUNT NUMBER,
    LINE_ITEMS_WITH_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    LINE_ITEMS_WITH_MANUF_COUPON_DISCOUNT NUMBER,
    LINE_ITEMS_WITH_TOTAL_COUPON_DISCOUNT NUMBER,
    BASKETS_PER_DAY NUMBER,
    PRODUCTS_PER_DAY NUMBER,
    LINE_ITEMS_PER_DAY NUMBER,
    AMOUNT_LIST_PER_DAY NUMBER,
    INSTORE_DISCOUNT_PER_DAY NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_PER_DAY NUMBER,
    MANUF_COUPON_DISCOUNT_PER_DAY NUMBER,
    TOTAL_COUPON_DISCOUNT_PER_DAY NUMBER,
    AMOUNT_PAID_PER_DAY NUMBER,
    DAYS_WITH_INSTORE_DISCOUNT_PER_DAYS NUMBER,
    DAYS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_DAYS NUMBER,
    DAYS_WITH_MANUF_COUPON_DISCOUNT_PER_DAYS NUMBER,
    DAYS_WITH_TOTAL_COUPON_DISCOUNT_PER_DAYS NUMBER,
    PRODUCTS_PER_BASKET NUMBER,
    LINE_ITEMS_PER_BASKET NUMBER,
    AMOUNT_LIST_PER_BASKET NUMBER,
    INSTORE_DISCOUNT_PER_BASKET NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_PER_BASKET NUMBER,
    MANUF_COUPON_DISCOUNT_PER_BASKET NUMBER,
    TOTAL_COUPON_DISCOUNT_PER_BASKET NUMBER,
    AMOUNT_PAID_PER_BASKET NUMBER,
    BASKETS_WITH_INSTORE_DISCOUNT_PER_BASKETS NUMBER,
    BASKETS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_BASKETS NUMBER,
    BASKETS_WITH_MANUF_COUPON_DISCOUNT_PER_BASKETS NUMBER,
    BASKETS_WITH_TOTAL_COUPON_DISCOUNT_PER_BASKETS NUMBER,
    LINE_ITEMS_PER_PRODUCT NUMBER,
    AMOUNT_LIST_PER_PRODUCT NUMBER,
    INSTORE_DISCOUNT_PER_PRODUCT NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_PER_PRODUCT NUMBER,
    MANUF_COUPON_DISCOUNT_PER_PRODUCT NUMBER,
    TOTAL_COUPON_DISCOUNT_PER_PRODUCT NUMBER,
    AMOUNT_PAID_PER_PRODUCT NUMBER,
    PRODUCTS_WITH_INSTORE_DISCOUNT_PER_PRODUCTS NUMBER,
    PRODUCTS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_PRODUCTS NUMBER,
    PRODUCTS_WITH_MANUF_COUPON_DISCOUNT_PER_PRODUCTS NUMBER,
    PRODUCTS_WITH_TOTAL_COUPON_DISCOUNT_PER_PRODUCTS NUMBER,
    AMOUNT_LIST_PER_LINE_ITEM NUMBER,
    INSTORE_DISCOUNT_PER_LINE_ITEM NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_PER_LINE_ITEM NUMBER,
    MANUF_COUPON_DISCOUNT_PER_LINE_ITEM NUMBER,
    TOTAL_COUPON_DISCOUNT_PER_LINE_ITEM NUMBER,
    AMOUNT_PAID_PER_LINE_ITEM NUMBER,
    LINE_ITEMS_WITH_INSTORE_DISCOUNT_PER_LINE_ITEMS NUMBER,
    LINE_ITEMS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_LINE_ITEMS NUMBER,
    LINE_ITEMS_WITH_MANUF_COUPON_DISCOUNT_PER_LINE_ITEMS NUMBER,
    LINE_ITEMS_WITH_TOTAL_COUPON_DISCOUNT_PER_LINE_ITEMS NUMBER,
    PRD_DAYS NUMBER,
    PRD_BASKETS NUMBER,
    PRD_PRODUCTS NUMBER,
    PRD_LINE_ITEMS NUMBER,
    PRD_AMOUNT_LIST NUMBER,
    PRD_INSTORE_DISCOUNT NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    PRD_MANUF_COUPON_DISCOUNT NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT NUMBER,
    PRD_AMOUNT_PAID NUMBER,
    PRD_DAYS_WITH_INSTORE_DISCOUNT NUMBER,
    PRD_DAYS_WITH_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    PRD_DAYS_WITH_MANUF_COUPON_DISCOUNT NUMBER,
    PRD_DAYS_WITH_TOTAL_COUPON_DISCOUNT NUMBER,
    PRD_BASKETS_WITH_INSTORE_DISCOUNT NUMBER,
    PRD_BASKETS_WITH_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    PRD_BASKETS_WITH_MANUF_COUPON_DISCOUNT NUMBER,
    PRD_BASKETS_WITH_TOTAL_COUPON_DISCOUNT NUMBER,
    PRD_PRODUCTS_WITH_INSTORE_DISCOUNT NUMBER,
    PRD_PRODUCTS_WITH_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    PRD_PRODUCTS_WITH_MANUF_COUPON_DISCOUNT NUMBER,
    PRD_PRODUCTS_WITH_TOTAL_COUPON_DISCOUNT NUMBER,
    PRD_LINE_ITEMS_WITH_INSTORE_DISCOUNT NUMBER,
    PRD_LINE_ITEMS_WITH_CAMPAIGN_COUPON_DISCOUNT NUMBER,
    PRD_LINE_ITEMS_WITH_MANUF_COUPON_DISCOUNT NUMBER,
    PRD_LINE_ITEMS_WITH_TOTAL_COUPON_DISCOUNT NUMBER,
    PRD_BASKETS_PER_DAY NUMBER,
    PRD_PRODUCTS_PER_DAY NUMBER,
    PRD_LINE_ITEMS_PER_DAY NUMBER,
    PRD_AMOUNT_LIST_PER_DAY NUMBER,
    PRD_INSTORE_DISCOUNT_PER_DAY NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_PER_DAY NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_PER_DAY NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_PER_DAY NUMBER,
    PRD_AMOUNT_PAID_PER_DAY NUMBER,
    PRD_DAYS_WITH_INSTORE_DISCOUNT_PER_DAYS NUMBER,
    PRD_DAYS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_DAYS NUMBER,
    PRD_DAYS_WITH_MANUF_COUPON_DISCOUNT_PER_DAYS NUMBER,
    PRD_DAYS_WITH_TOTAL_COUPON_DISCOUNT_PER_DAYS NUMBER,
    PRD_PRODUCTS_PER_BASKET NUMBER,
    PRD_LINE_ITEMS_PER_BASKET NUMBER,
    PRD_AMOUNT_LIST_PER_BASKET NUMBER,
    PRD_INSTORE_DISCOUNT_PER_BASKET NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_PER_BASKET NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_PER_BASKET NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_PER_BASKET NUMBER,
    PRD_AMOUNT_PAID_PER_BASKET NUMBER,
    PRD_BASKETS_WITH_INSTORE_DISCOUNT_PER_BASKETS NUMBER,
    PRD_BASKETS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_BASKETS NUMBER,
    PRD_BASKETS_WITH_MANUF_COUPON_DISCOUNT_PER_BASKETS NUMBER,
    PRD_BASKETS_WITH_TOTAL_COUPON_DISCOUNT_PER_BASKETS NUMBER,
    PRD_LINE_ITEMS_PER_PRODUCT NUMBER,
    PRD_AMOUNT_LIST_PER_PRODUCT NUMBER,
    PRD_INSTORE_DISCOUNT_PER_PRODUCT NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_PER_PRODUCT NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_PER_PRODUCT NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_PER_PRODUCT NUMBER,
    PRD_AMOUNT_PAID_PER_PRODUCT NUMBER,
    PRD_PRODUCTS_WITH_INSTORE_DISCOUNT_PER_PRODUCTS NUMBER,
    PRD_PRODUCTS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_PRODUCTS NUMBER,
    PRD_PRODUCTS_WITH_MANUF_COUPON_DISCOUNT_PER_PRODUCTS NUMBER,
    PRD_PRODUCTS_WITH_TOTAL_COUPON_DISCOUNT_PER_PRODUCTS NUMBER,
    PRD_AMOUNT_LIST_PER_LINE_ITEM NUMBER,
    PRD_INSTORE_DISCOUNT_PER_LINE_ITEM NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_PER_LINE_ITEM NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_PER_LINE_ITEM NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_PER_LINE_ITEM NUMBER,
    PRD_AMOUNT_PAID_PER_LINE_ITEM NUMBER,
    PRD_LINE_ITEMS_WITH_INSTORE_DISCOUNT_PER_LINE_ITEMS NUMBER,
    PRD_LINE_ITEMS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_LINE_ITEMS NUMBER,
    PRD_LINE_ITEMS_WITH_MANUF_COUPON_DISCOUNT_PER_LINE_ITEMS NUMBER,
    PRD_LINE_ITEMS_WITH_TOTAL_COUPON_DISCOUNT_PER_LINE_ITEMS NUMBER,
    PURCHASED NUMBER);


-- Populate data
TRUNCATE TABLE CHARLIE_FEATURE_STORE;
TRUNCATE TABLE CHARLIE_FEATURES;

-- Create 60-day feature sets
CALL CREATE_FEATURE_STORE(1);
CALL CREATE_FEATURE_STORE(31);
CALL CREATE_FEATURE_STORE(61);
CALL CREATE_FEATURE_STORE(91);
CALL CREATE_FEATURE_STORE(121);

-- Consolidate feature sets
CALL CREATE_FEATURES(1);