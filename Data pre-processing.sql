-- Create a view for the adjusted transaction data
CREATE OR REPLACE VIEW CHARLIE_TRANSACTION_DATA_ADJ AS
SELECT
    t.household_key,
    t.basket_id,
    t.week_no,
    t.day,
    current_date() + (t.day - 712) AS date,
    t.trans_time,
    t.store_id,
    t.product_id,
    
    p.commodity_desc,

    COALESCE(t.sales_value - t.retail_disc - t.coupon_disc - t.coupon_match_disc, 0.0) as amount_list,
    CASE 
        WHEN COALESCE(t.coupon_match_disc, 0.0) = 0.0 THEN -1 * COALESCE(t.coupon_disc, 0.0) 
        ELSE 0.0 
    END as campaign_coupon_discount,
    
    CASE 
        WHEN COALESCE(t.coupon_match_disc, 0.0) != 0.0 THEN -1 * COALESCE(t.coupon_disc, 0.0) 
        ELSE 0.0 
    END as manuf_coupon_discount,
    
    -1 * COALESCE(t.coupon_match_disc,0.0) as manuf_coupon_match_discount,
    -1 * COALESCE(t.coupon_disc - t.coupon_match_disc,0.0) as total_coupon_discount,
    COALESCE(-1 * t.retail_disc,0.0) as instore_discount,
    COALESCE(t.sales_value,0.0) as amount_paid,
    t.quantity as units

FROM
    CHARLIE_TRANSACTION_DATA t
    INNER JOIN CHARLIE_PRODUCT p
    ON t.PRODUCT_ID = p.PRODUCT_ID
    
    
WHERE HOUSEHOLD_KEY IN (SELECT DISTINCT HOUSEHOLD_KEY FROM CHARLIE_HH_DEMOGRAPHIC);


-- Procedure to create the updated Feature Store
CREATE OR REPLACE PROCEDURE CREATE_FEATURE_SETS(SIZES array, LAGS array)
      returns string not null
      language python
      runtime_version = '3.8'
      packages = ('snowflake-snowpark-python')
      handler = 'execute'
    as
$$
def execute(snowpark_session, SIZES: list, LAGS: list):

    ## Create a size table for each size
    for SIZE in SIZES:
        snowpark_session.sql(f'CALL CREATE_FEATURE_TABLE({SIZE})').collect()
        snowpark_session.sql(f'CALL CREATE_INFERENCE_TABLE({SIZE})').collect()

        ## Populate tables with data for each time lag
        for LAG in LAGS:
            snowpark_session.sql(f'CALL CREATE_FEATURE_SET({SIZE}, {LAG})').collect()

        ## Populate inference set
        snowpark_session.sql(f'CALL CREATE_INFERENCE_SET({SIZE})').collect()

    ## Create a combined dataset for all sizes
    if len(SIZES) == 1:
        feature_text = f"CREATE OR REPLACE TABLE CHARLIE_FEATURE_STORE AS SELECT * FROM CHARLIE_FEATURES_{SIZES[0]}_DAYS"
        inference_text = f"CREATE OR REPLACE TABLE CHARLIE_INFERENCE_STORE AS SELECT * FROM CHARLIE_INFERENCE_{SIZES[0]}_DAYS"

    else:
        feature_text = f"CREATE OR REPLACE TABLE CHARLIE_FEATURE_STORE AS SELECT * FROM CHARLIE_FEATURES_{SIZES[0]}_DAYS " 
        inference_text = f"CREATE OR REPLACE TABLE CHARLIE_INFERENCE_STORE AS SELECT * FROM CHARLIE_INFERENCE_{SIZES[0]}_DAYS " 
        for SIZE in SIZES[1:]:
            feature_text += f'NATURAL JOIN (SELECT * EXCLUDE PURCHASED FROM CHARLIE_FEATURES_{SIZE}_DAYS) "{SIZE}_DAYS" '
            inference_text += f'NATURAL JOIN (SELECT * EXCLUDE PURCHASED FROM CHARLIE_INFERENCE_{SIZE}_DAYS) "{SIZE}_DAYS" '
            
    snowpark_session.sql(feature_text).collect()
    snowpark_session.sql(inference_text).collect()

    # Remove tables
    for SIZE in SIZES:
        snowpark_session.sql(f"DROP TABLE CHARLIE_FEATURES_{SIZE}_DAYS").collect()
        snowpark_session.sql(f"DROP TABLE CHARLIE_INFERENCE_{SIZE}_DAYS").collect()
    
    
    return "Complete!"
      
$$;



-- Proc for creating a feature table for a given size
CREATE OR REPLACE PROCEDURE CREATE_FEATURE_TABLE(SIZE varchar)
    returns string
    language javascript
    strict

    as
    $$
    var sql_command = 
    
    `
    CREATE OR REPLACE TABLE CHARLIE_FEATURES_` + SIZE + `_DAYS
    (DATE DATE,
    HOUSEHOLD_KEY NUMBER,
    COMMODITY_DESC VARCHAR,
    DAYS_` + SIZE + ` NUMBER,
    BASKETS_` + SIZE + ` NUMBER,
    PRODUCTS_` + SIZE + ` NUMBER,
    LINE_ITEMS_` + SIZE + ` NUMBER,
    AMOUNT_LIST_` + SIZE + ` NUMBER,
    INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    AMOUNT_PAID_` + SIZE + ` NUMBER,
    DAYS_WITH_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    DAYS_WITH_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    DAYS_WITH_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    DAYS_WITH_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    BASKETS_WITH_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    BASKETS_WITH_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    BASKETS_WITH_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    BASKETS_WITH_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRODUCTS_WITH_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    PRODUCTS_WITH_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRODUCTS_WITH_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRODUCTS_WITH_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    LINE_ITEMS_WITH_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    LINE_ITEMS_WITH_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    LINE_ITEMS_WITH_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    LINE_ITEMS_WITH_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    BASKETS_PER_DAY_` + SIZE + ` NUMBER,
    PRODUCTS_PER_DAY_` + SIZE + ` NUMBER,
    LINE_ITEMS_PER_DAY_` + SIZE + ` NUMBER,
    AMOUNT_LIST_PER_DAY_` + SIZE + ` NUMBER,
    INSTORE_DISCOUNT_PER_DAY_` + SIZE + ` NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_PER_DAY_` + SIZE + ` NUMBER,
    MANUF_COUPON_DISCOUNT_PER_DAY_` + SIZE + ` NUMBER,
    TOTAL_COUPON_DISCOUNT_PER_DAY_` + SIZE + ` NUMBER,
    AMOUNT_PAID_PER_DAY_` + SIZE + ` NUMBER,
    DAYS_WITH_INSTORE_DISCOUNT_PER_DAYS_` + SIZE + ` NUMBER,
    DAYS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_DAYS_` + SIZE + ` NUMBER,
    DAYS_WITH_MANUF_COUPON_DISCOUNT_PER_DAYS_` + SIZE + ` NUMBER,
    DAYS_WITH_TOTAL_COUPON_DISCOUNT_PER_DAYS_` + SIZE + ` NUMBER,
    PRODUCTS_PER_BASKET_` + SIZE + ` NUMBER,
    LINE_ITEMS_PER_BASKET_` + SIZE + ` NUMBER,
    AMOUNT_LIST_PER_BASKET_` + SIZE + ` NUMBER,
    INSTORE_DISCOUNT_PER_BASKET_` + SIZE + ` NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_PER_BASKET_` + SIZE + ` NUMBER,
    MANUF_COUPON_DISCOUNT_PER_BASKET_` + SIZE + ` NUMBER,
    TOTAL_COUPON_DISCOUNT_PER_BASKET_` + SIZE + ` NUMBER,
    AMOUNT_PAID_PER_BASKET_` + SIZE + ` NUMBER,
    BASKETS_WITH_INSTORE_DISCOUNT_PER_BASKETS_` + SIZE + ` NUMBER,
    BASKETS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_BASKETS_` + SIZE + ` NUMBER,
    BASKETS_WITH_MANUF_COUPON_DISCOUNT_PER_BASKETS_` + SIZE + ` NUMBER,
    BASKETS_WITH_TOTAL_COUPON_DISCOUNT_PER_BASKETS_` + SIZE + ` NUMBER,
    LINE_ITEMS_PER_PRODUCT_` + SIZE + ` NUMBER,
    AMOUNT_LIST_PER_PRODUCT_` + SIZE + ` NUMBER,
    INSTORE_DISCOUNT_PER_PRODUCT_` + SIZE + ` NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_PER_PRODUCT_` + SIZE + ` NUMBER,
    MANUF_COUPON_DISCOUNT_PER_PRODUCT_` + SIZE + ` NUMBER,
    TOTAL_COUPON_DISCOUNT_PER_PRODUCT_` + SIZE + ` NUMBER,
    AMOUNT_PAID_PER_PRODUCT_` + SIZE + ` NUMBER,
    PRODUCTS_WITH_INSTORE_DISCOUNT_PER_PRODUCTS_` + SIZE + ` NUMBER,
    PRODUCTS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_PRODUCTS_` + SIZE + ` NUMBER,
    PRODUCTS_WITH_MANUF_COUPON_DISCOUNT_PER_PRODUCTS_` + SIZE + ` NUMBER,
    PRODUCTS_WITH_TOTAL_COUPON_DISCOUNT_PER_PRODUCTS_` + SIZE + ` NUMBER,
    AMOUNT_LIST_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    INSTORE_DISCOUNT_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    MANUF_COUPON_DISCOUNT_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    TOTAL_COUPON_DISCOUNT_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    AMOUNT_PAID_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    LINE_ITEMS_WITH_INSTORE_DISCOUNT_PER_LINE_ITEMS_` + SIZE + ` NUMBER,
    LINE_ITEMS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_LINE_ITEMS_` + SIZE + ` NUMBER,
    LINE_ITEMS_WITH_MANUF_COUPON_DISCOUNT_PER_LINE_ITEMS_` + SIZE + ` NUMBER,
    LINE_ITEMS_WITH_TOTAL_COUPON_DISCOUNT_PER_LINE_ITEMS_` + SIZE + ` NUMBER,
    PRD_DAYS_` + SIZE + ` NUMBER,
    PRD_BASKETS_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_` + SIZE + ` NUMBER,
    PRD_AMOUNT_LIST_` + SIZE + ` NUMBER,
    PRD_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_AMOUNT_PAID_` + SIZE + ` NUMBER,
    PRD_DAYS_WITH_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_DAYS_WITH_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_DAYS_WITH_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_DAYS_WITH_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_BASKETS_WITH_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_BASKETS_WITH_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_BASKETS_WITH_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_BASKETS_WITH_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_WITH_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_WITH_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_WITH_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_WITH_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_WITH_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_WITH_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_WITH_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_WITH_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_BASKETS_PER_DAY_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_PER_DAY_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_PER_DAY_` + SIZE + ` NUMBER,
    PRD_AMOUNT_LIST_PER_DAY_` + SIZE + ` NUMBER,
    PRD_INSTORE_DISCOUNT_PER_DAY_` + SIZE + ` NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_PER_DAY_` + SIZE + ` NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_PER_DAY_` + SIZE + ` NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_PER_DAY_` + SIZE + ` NUMBER,
    PRD_AMOUNT_PAID_PER_DAY_` + SIZE + ` NUMBER,
    PRD_DAYS_WITH_INSTORE_DISCOUNT_PER_DAYS_` + SIZE + ` NUMBER,
    PRD_DAYS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_DAYS_` + SIZE + ` NUMBER,
    PRD_DAYS_WITH_MANUF_COUPON_DISCOUNT_PER_DAYS_` + SIZE + ` NUMBER,
    PRD_DAYS_WITH_TOTAL_COUPON_DISCOUNT_PER_DAYS_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_PER_BASKET_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_PER_BASKET_` + SIZE + ` NUMBER,
    PRD_AMOUNT_LIST_PER_BASKET_` + SIZE + ` NUMBER,
    PRD_INSTORE_DISCOUNT_PER_BASKET_` + SIZE + ` NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_PER_BASKET_` + SIZE + ` NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_PER_BASKET_` + SIZE + ` NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_PER_BASKET_` + SIZE + ` NUMBER,
    PRD_AMOUNT_PAID_PER_BASKET_` + SIZE + ` NUMBER,
    PRD_BASKETS_WITH_INSTORE_DISCOUNT_PER_BASKETS_` + SIZE + ` NUMBER,
    PRD_BASKETS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_BASKETS_` + SIZE + ` NUMBER,
    PRD_BASKETS_WITH_MANUF_COUPON_DISCOUNT_PER_BASKETS_` + SIZE + ` NUMBER,
    PRD_BASKETS_WITH_TOTAL_COUPON_DISCOUNT_PER_BASKETS_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_PER_PRODUCT_` + SIZE + ` NUMBER,
    PRD_AMOUNT_LIST_PER_PRODUCT_` + SIZE + ` NUMBER,
    PRD_INSTORE_DISCOUNT_PER_PRODUCT_` + SIZE + ` NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_PER_PRODUCT_` + SIZE + ` NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_PER_PRODUCT_` + SIZE + ` NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_PER_PRODUCT_` + SIZE + ` NUMBER,
    PRD_AMOUNT_PAID_PER_PRODUCT_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_WITH_INSTORE_DISCOUNT_PER_PRODUCTS_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_PRODUCTS_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_WITH_MANUF_COUPON_DISCOUNT_PER_PRODUCTS_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_WITH_TOTAL_COUPON_DISCOUNT_PER_PRODUCTS_` + SIZE + ` NUMBER,
    PRD_AMOUNT_LIST_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    PRD_INSTORE_DISCOUNT_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    PRD_AMOUNT_PAID_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_WITH_INSTORE_DISCOUNT_PER_LINE_ITEMS_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_LINE_ITEMS_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_WITH_MANUF_COUPON_DISCOUNT_PER_LINE_ITEMS_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_WITH_TOTAL_COUPON_DISCOUNT_PER_LINE_ITEMS_` + SIZE + ` NUMBER,
    PURCHASED NUMBER);
        
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
    $$;

    
-- Proc for creating a 30 fay feature set for a given lag (t-1, t-2)
CREATE OR REPLACE PROCEDURE CREATE_FEATURE_SET(SIZE varchar, LAG float)
    returns string
    language javascript
    strict
    as
    
    $$
    var sql_command =
    
    `
    INSERT INTO CHARLIE_FEATURES_` + SIZE + `_DAYS
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
    
        WHERE (CURRENT_DATE() - ` + LAG + ` - 29 >= DATE) AND (CURRENT_DATE() - ` + LAG + ` - 29 - ` + SIZE + ` <= DATE)
    
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
    
        WHERE (CURRENT_DATE() - ` + LAG + ` - 29 >= DATE) AND (CURRENT_DATE() - ` + LAG + ` - 29 - ` + SIZE + ` <= DATE)
    
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






-- Proc for creating a feature table for a given size
CREATE OR REPLACE PROCEDURE CREATE_INFERENCE_TABLE(SIZE varchar)
    returns string
    language javascript
    strict

    as
    $$
    var sql_command = 
    
    `
    CREATE OR REPLACE TABLE CHARLIE_INFERENCE_` + SIZE + `_DAYS
    (DATE DATE,
    HOUSEHOLD_KEY NUMBER,
    COMMODITY_DESC VARCHAR,
    DAYS_` + SIZE + ` NUMBER,
    BASKETS_` + SIZE + ` NUMBER,
    PRODUCTS_` + SIZE + ` NUMBER,
    LINE_ITEMS_` + SIZE + ` NUMBER,
    AMOUNT_LIST_` + SIZE + ` NUMBER,
    INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    AMOUNT_PAID_` + SIZE + ` NUMBER,
    DAYS_WITH_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    DAYS_WITH_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    DAYS_WITH_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    DAYS_WITH_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    BASKETS_WITH_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    BASKETS_WITH_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    BASKETS_WITH_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    BASKETS_WITH_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRODUCTS_WITH_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    PRODUCTS_WITH_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRODUCTS_WITH_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRODUCTS_WITH_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    LINE_ITEMS_WITH_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    LINE_ITEMS_WITH_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    LINE_ITEMS_WITH_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    LINE_ITEMS_WITH_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    BASKETS_PER_DAY_` + SIZE + ` NUMBER,
    PRODUCTS_PER_DAY_` + SIZE + ` NUMBER,
    LINE_ITEMS_PER_DAY_` + SIZE + ` NUMBER,
    AMOUNT_LIST_PER_DAY_` + SIZE + ` NUMBER,
    INSTORE_DISCOUNT_PER_DAY_` + SIZE + ` NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_PER_DAY_` + SIZE + ` NUMBER,
    MANUF_COUPON_DISCOUNT_PER_DAY_` + SIZE + ` NUMBER,
    TOTAL_COUPON_DISCOUNT_PER_DAY_` + SIZE + ` NUMBER,
    AMOUNT_PAID_PER_DAY_` + SIZE + ` NUMBER,
    DAYS_WITH_INSTORE_DISCOUNT_PER_DAYS_` + SIZE + ` NUMBER,
    DAYS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_DAYS_` + SIZE + ` NUMBER,
    DAYS_WITH_MANUF_COUPON_DISCOUNT_PER_DAYS_` + SIZE + ` NUMBER,
    DAYS_WITH_TOTAL_COUPON_DISCOUNT_PER_DAYS_` + SIZE + ` NUMBER,
    PRODUCTS_PER_BASKET_` + SIZE + ` NUMBER,
    LINE_ITEMS_PER_BASKET_` + SIZE + ` NUMBER,
    AMOUNT_LIST_PER_BASKET_` + SIZE + ` NUMBER,
    INSTORE_DISCOUNT_PER_BASKET_` + SIZE + ` NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_PER_BASKET_` + SIZE + ` NUMBER,
    MANUF_COUPON_DISCOUNT_PER_BASKET_` + SIZE + ` NUMBER,
    TOTAL_COUPON_DISCOUNT_PER_BASKET_` + SIZE + ` NUMBER,
    AMOUNT_PAID_PER_BASKET_` + SIZE + ` NUMBER,
    BASKETS_WITH_INSTORE_DISCOUNT_PER_BASKETS_` + SIZE + ` NUMBER,
    BASKETS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_BASKETS_` + SIZE + ` NUMBER,
    BASKETS_WITH_MANUF_COUPON_DISCOUNT_PER_BASKETS_` + SIZE + ` NUMBER,
    BASKETS_WITH_TOTAL_COUPON_DISCOUNT_PER_BASKETS_` + SIZE + ` NUMBER,
    LINE_ITEMS_PER_PRODUCT_` + SIZE + ` NUMBER,
    AMOUNT_LIST_PER_PRODUCT_` + SIZE + ` NUMBER,
    INSTORE_DISCOUNT_PER_PRODUCT_` + SIZE + ` NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_PER_PRODUCT_` + SIZE + ` NUMBER,
    MANUF_COUPON_DISCOUNT_PER_PRODUCT_` + SIZE + ` NUMBER,
    TOTAL_COUPON_DISCOUNT_PER_PRODUCT_` + SIZE + ` NUMBER,
    AMOUNT_PAID_PER_PRODUCT_` + SIZE + ` NUMBER,
    PRODUCTS_WITH_INSTORE_DISCOUNT_PER_PRODUCTS_` + SIZE + ` NUMBER,
    PRODUCTS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_PRODUCTS_` + SIZE + ` NUMBER,
    PRODUCTS_WITH_MANUF_COUPON_DISCOUNT_PER_PRODUCTS_` + SIZE + ` NUMBER,
    PRODUCTS_WITH_TOTAL_COUPON_DISCOUNT_PER_PRODUCTS_` + SIZE + ` NUMBER,
    AMOUNT_LIST_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    INSTORE_DISCOUNT_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    CAMPAIGN_COUPON_DISCOUNT_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    MANUF_COUPON_DISCOUNT_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    TOTAL_COUPON_DISCOUNT_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    AMOUNT_PAID_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    LINE_ITEMS_WITH_INSTORE_DISCOUNT_PER_LINE_ITEMS_` + SIZE + ` NUMBER,
    LINE_ITEMS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_LINE_ITEMS_` + SIZE + ` NUMBER,
    LINE_ITEMS_WITH_MANUF_COUPON_DISCOUNT_PER_LINE_ITEMS_` + SIZE + ` NUMBER,
    LINE_ITEMS_WITH_TOTAL_COUPON_DISCOUNT_PER_LINE_ITEMS_` + SIZE + ` NUMBER,
    PRD_DAYS_` + SIZE + ` NUMBER,
    PRD_BASKETS_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_` + SIZE + ` NUMBER,
    PRD_AMOUNT_LIST_` + SIZE + ` NUMBER,
    PRD_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_AMOUNT_PAID_` + SIZE + ` NUMBER,
    PRD_DAYS_WITH_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_DAYS_WITH_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_DAYS_WITH_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_DAYS_WITH_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_BASKETS_WITH_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_BASKETS_WITH_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_BASKETS_WITH_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_BASKETS_WITH_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_WITH_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_WITH_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_WITH_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_WITH_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_WITH_INSTORE_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_WITH_CAMPAIGN_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_WITH_MANUF_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_WITH_TOTAL_COUPON_DISCOUNT_` + SIZE + ` NUMBER,
    PRD_BASKETS_PER_DAY_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_PER_DAY_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_PER_DAY_` + SIZE + ` NUMBER,
    PRD_AMOUNT_LIST_PER_DAY_` + SIZE + ` NUMBER,
    PRD_INSTORE_DISCOUNT_PER_DAY_` + SIZE + ` NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_PER_DAY_` + SIZE + ` NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_PER_DAY_` + SIZE + ` NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_PER_DAY_` + SIZE + ` NUMBER,
    PRD_AMOUNT_PAID_PER_DAY_` + SIZE + ` NUMBER,
    PRD_DAYS_WITH_INSTORE_DISCOUNT_PER_DAYS_` + SIZE + ` NUMBER,
    PRD_DAYS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_DAYS_` + SIZE + ` NUMBER,
    PRD_DAYS_WITH_MANUF_COUPON_DISCOUNT_PER_DAYS_` + SIZE + ` NUMBER,
    PRD_DAYS_WITH_TOTAL_COUPON_DISCOUNT_PER_DAYS_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_PER_BASKET_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_PER_BASKET_` + SIZE + ` NUMBER,
    PRD_AMOUNT_LIST_PER_BASKET_` + SIZE + ` NUMBER,
    PRD_INSTORE_DISCOUNT_PER_BASKET_` + SIZE + ` NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_PER_BASKET_` + SIZE + ` NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_PER_BASKET_` + SIZE + ` NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_PER_BASKET_` + SIZE + ` NUMBER,
    PRD_AMOUNT_PAID_PER_BASKET_` + SIZE + ` NUMBER,
    PRD_BASKETS_WITH_INSTORE_DISCOUNT_PER_BASKETS_` + SIZE + ` NUMBER,
    PRD_BASKETS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_BASKETS_` + SIZE + ` NUMBER,
    PRD_BASKETS_WITH_MANUF_COUPON_DISCOUNT_PER_BASKETS_` + SIZE + ` NUMBER,
    PRD_BASKETS_WITH_TOTAL_COUPON_DISCOUNT_PER_BASKETS_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_PER_PRODUCT_` + SIZE + ` NUMBER,
    PRD_AMOUNT_LIST_PER_PRODUCT_` + SIZE + ` NUMBER,
    PRD_INSTORE_DISCOUNT_PER_PRODUCT_` + SIZE + ` NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_PER_PRODUCT_` + SIZE + ` NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_PER_PRODUCT_` + SIZE + ` NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_PER_PRODUCT_` + SIZE + ` NUMBER,
    PRD_AMOUNT_PAID_PER_PRODUCT_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_WITH_INSTORE_DISCOUNT_PER_PRODUCTS_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_PRODUCTS_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_WITH_MANUF_COUPON_DISCOUNT_PER_PRODUCTS_` + SIZE + ` NUMBER,
    PRD_PRODUCTS_WITH_TOTAL_COUPON_DISCOUNT_PER_PRODUCTS_` + SIZE + ` NUMBER,
    PRD_AMOUNT_LIST_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    PRD_INSTORE_DISCOUNT_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    PRD_CAMPAIGN_COUPON_DISCOUNT_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    PRD_MANUF_COUPON_DISCOUNT_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    PRD_TOTAL_COUPON_DISCOUNT_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    PRD_AMOUNT_PAID_PER_LINE_ITEM_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_WITH_INSTORE_DISCOUNT_PER_LINE_ITEMS_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_WITH_CAMPAIGN_COUPON_DISCOUNT_PER_LINE_ITEMS_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_WITH_MANUF_COUPON_DISCOUNT_PER_LINE_ITEMS_` + SIZE + ` NUMBER,
    PRD_LINE_ITEMS_WITH_TOTAL_COUPON_DISCOUNT_PER_LINE_ITEMS_` + SIZE + ` NUMBER,
    PURCHASED NUMBER);
        
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
    $$;



SELECT * FROM CHARLIE_INFERENCE_30_DAYS;
    
-- Proc for creating a 30 fay feature set for a given lag (t-1, t-2)
CREATE OR REPLACE PROCEDURE CREATE_INFERENCE_SET(SIZE varchar)
    returns string
    language javascript
    strict
    as
    
    $$
    var sql_command =
    
    `
    INSERT INTO CHARLIE_INFERENCE_` + SIZE + `_DAYS
    -- Get label for t-1 <-> t-30 days
    SELECT 
        -- Date
        CURRENT_DATE - 1 as DATE,
    
        -- Household and product
        h.household_key,
        p.commodity_desc,
    
        -- Household only features
        features_hh.* EXCLUDE household_key,
    
        -- Household and product pairing features
        features_hh_prd.* EXCLUDE (household_key, commodity_desc),
    
        -- Label
    	NULL as PURCHASED
    
    FROM CHARLIE_HH_DEMOGRAPHIC h
    
    CROSS JOIN (SELECT DISTINCT commodity_desc FROM CHARLIE_PRODUCT) p
    
    -- Get purchased label
    LEFT JOIN 
    
        (SELECT DISTINCT
        household_key,
        COMMODITY_DESC
        FROM CHARLIE_TRANSACTION_DATA_ADJ
        WHERE (CURRENT_DATE() - 1 >= DATE) AND (CURRENT_DATE() - 30 <= DATE)) t
    
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
    
        WHERE (CURRENT_DATE() - 1 >= DATE) AND (CURRENT_DATE() - ` + SIZE + ` <= DATE)
    
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
    
        WHERE (CURRENT_DATE() - 1 >= DATE) AND (CURRENT_DATE() - ` + SIZE + ` <= DATE)
    
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


CALL CREATE_FEATURE_SETS(['30', '60', '90'], [1, 31, 61, 91]);


-- Procedure to create the updated Feature Store
CREATE OR REPLACE PROCEDURE TRAIN_PROPENSITY_MODEL(PRODUCT varchar)
      returns string 
      language python
      runtime_version = '3.8'
      packages = ('snowflake-snowpark-python', 'tensorflow', 'pandas', 'numpy')
      handler = 'execute'
    as
$$
import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras import layers
from snowflake.snowpark import DataFrameWriter

def execute(snowpark_session, PRODUCT: str):

	## Get features and inference stores
    feature_data = snowpark_session.sql(f"SELECT * FROM CHARLIE_FEATURE_STORE WHERE COMMODITY_DESC = '{PRODUCT}';").to_pandas()
    inference_data = snowpark_session.sql(f"SELECT * FROM CHARLIE_INFERENCE_STORE WHERE COMMODITY_DESC = '{PRODUCT}';").to_pandas()


    def process_tensors(data):
		
        # Subset for chosen product to model
        data = data.drop(['COMMODITY_DESC', 'DATE', 'HOUSEHOLD_KEY'], axis = 1)
        
        # reset index to ensure it can be rejoined later
        data = data.reset_index(drop = True)
        
        # Convert to float - needed for TF
        data['PURCHASED'] = data['PURCHASED'].astype(float)
		
        # Remove NaN
        data = data.fillna(0)
		
        # Split to features and labels
        data_x = data.drop(['PURCHASED'], axis = 1)
        data_y = data['PURCHASED']
		
        return data_x, data_y

    # Pre-process the data and test data
    ds_feature_data_x, ds_feature_data_y = process_tensors(feature_data)
    ds_inference_data_x, ds_inference_data_y = process_tensors(inference_data)

    # Define and train TF model
    model = tf.keras.Sequential(
        [layers.Dense(128, activation = 'relu'),
        layers.Dense(128, activation = 'relu'),
        layers.Dense(128, activation = 'relu'),
        layers.Dense(1)]
        )

    model.compile(
        optimizer='adam',
        loss=tf.keras.losses.BinaryCrossentropy(from_logits=True),
        metrics=["accuracy", "AUC"])

    # Model training
    model.fit(ds_feature_data_x.to_numpy(), ds_feature_data_y.to_numpy(), epochs=30)

    # Append predictions to dataset
    inference_data['PREDICTION'] = np.round(tf.nn.sigmoid(model.predict(ds_inference_data_x.to_numpy())), 4)

    result = snowpark_session.create_dataframe(inference_data[['HOUSEHOLD_KEY', 'PREDICTION']])

    # Write to table
    result.write.mode("overwrite").save_as_table("CHARLIE_INFERENCE_PREDICTIONS")
    snowpark_session.table("CHARLIE_INFERENCE_PREDICTIONS").collect()
    
    return "Success"
      
$$;