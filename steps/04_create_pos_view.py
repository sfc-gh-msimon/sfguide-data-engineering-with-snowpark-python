#------------------------------------------------------------------------------
# Hands-On Lab: Data Engineering with Snowpark
# Script:       04_create_order_view.py
# Author:       Jeremiah Hansen, Caleb Baechtold
# Last Updated: 1/9/2023
#------------------------------------------------------------------------------

# SNOWFLAKE ADVANTAGE: Snowpark DataFrame API -- Streams for incremental processing (CDC) -- Streams on views

import snowflake.snowpark.functions as F

def create_pos_view(session):
    session.use_schema('HARMONIZED')
    truck = session.table("RAW_POS.TRUCK")
    menu = session.table("RAW_POS.MENU")
    location = session.table("RAW_POS.LOCATION")
    order_detail = session.table("RAW_POS.ORDER_DETAIL")
    order_header = session.table("RAW_POS.ORDER_HEADER").select(F.col("ORDER_ID"), \
                                                                F.col("TRUCK_ID"), \
                                                                F.col("ORDER_TS"), \
                                                                F.to_date(F.col("ORDER_TS")).alias("ORDER_TS_DATE"), \
                                                                F.col("ORDER_AMOUNT"), \
                                                                F.col("ORDER_TAX_AMOUNT"), \
                                                                F.col("ORDER_DISCOUNT_AMOUNT"), \
                                                                F.col("LOCATION_ID"), \
                                                                F.col("ORDER_TOTAL"))
    franchise = session.table("RAW_POS.FRANCHISE").select(F.col("FRANCHISE_ID"), \
                                                          F.col("FIRST_NAME").alias("FRANCHISEE_FIRST_NAME"), \
                                                          F.col("LAST_NAME").alias("FRANCHISEE_LAST_NAME"))


    t_with_f = truck.join(franchise, truck['FRANCHISE_ID'] == franchise['FRANCHISE_ID'], rsuffix='_f')
    oh_w_t_and_l = order_header.join(t_with_f, order_header['TRUCK_ID'] == t_with_f['TRUCK_ID'], rsuffix='_t') \
                                .join(location, order_header['LOCATION_ID'] == location['LOCATION_ID'], rsuffix='_l')
    final_df = order_detail.join(oh_w_t_and_l, order_detail['ORDER_ID'] == oh_w_t_and_l['ORDER_ID'], rsuffix='_oh') \
                            .join(menu, order_detail['MENU_ITEM_ID'] == menu['MENU_ITEM_ID'], rsuffix='_m')
    final_df = final_df.select(F.col("ORDER_ID"), \
                            F.col("TRUCK_ID"), \
                            F.col("ORDER_TS"), \
                            F.col('ORDER_TS_DATE'), \
                            F.col("ORDER_DETAIL_ID"), \
                            F.col("LINE_NUMBER"), \
                            F.col("TRUCK_BRAND_NAME"), \
                            F.col("MENU_TYPE"), \
                            F.col("PRIMARY_CITY"), \
                            F.col("REGION"), \
                            F.col("COUNTRY"), \
                            F.col("FRANCHISE_FLAG"), \
                            F.col("FRANCHISE_ID"), \
                            F.col("FRANCHISEE_FIRST_NAME"), \
                            F.col("FRANCHISEE_LAST_NAME"), \
                            F.col("LOCATION_ID"), \
                            F.col("MENU_ITEM_ID"), \
                            F.col("MENU_ITEM_NAME"), \
                            F.col("QUANTITY"), \
                            F.col("UNIT_PRICE"), \
                            F.col("PRICE"), \
                            F.col("ORDER_AMOUNT"), \
                            F.col("ORDER_TAX_AMOUNT"), \
                            F.col("ORDER_DISCOUNT_AMOUNT"), \
                            F.col("ORDER_TOTAL"))
    final_df.create_or_replace_view('POS_FLATTENED_V')

def create_pos_view_stream(session):
    session.use_schema('HARMONIZED')
    _ = session.sql('CREATE OR REPLACE STREAM POS_FLATTENED_V_STREAM ON VIEW POS_FLATTENED_V SHOW_INITIAL_ROWS = TRUE').collect()



if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    create_pos_view(session)
    create_pos_view_stream(session)

    session.close()
