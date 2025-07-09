import cx_Oracle
import csv
from datetime import datetime
import os

def run_sql_query_to_csv():
    """
    Connects to an Oracle database, executes a predefined SQL query,
    and exports the results to a CSV file.

    This function mimics the SQL*Plus script's functionality by:
    1. Connecting to the specified Oracle database.
    2. Executing the main SELECT query.
    3. Formatting the output to a CSV file.
    4. Naming the CSV file based on the current date.

    NOTE: SQL*Plus specific commands like `SET ECHO OFF`, `COLUMN FORMAT`,
    `SPOOL ON/OFF` are handled by Python's `cx_Oracle` and `csv` modules,
    which manage data retrieval and file writing directly, making
    these SQL*Plus display/spooling commands unnecessary in the Python context.
    """

    # --- Database Connection Details ---
    # IMPORTANT: Replace these with your actual Oracle database credentials
    # and connection string (DSN).
    # Example DSN formats:
    #   - 'localhost:1521/ORCL' (for TNS-less connection)
    #   - 'user/password@host:port/service_name'
    #   - 'TNSNAME' (if you have a tnsnames.ora configured)
    username = 'apps'
    password = 'apps'
    dsn = '192.168.200.179/erpp' # This should match your Oracle TNS entry or a direct connection string

    # --- Generate Date for Filename ---
    # This replaces the `select to_char(sysdate,'YYYYMMDD') dcol from dual;` part
    current_date_str = datetime.now().strftime('%Y%m%d')
    output_filename = f'C:/NESTLE/NCMXTPLSH_{current_date_str}.csv'

    # --- SQL Query ---
    # This is your main SELECT statement.
    # Aliases and TRIM functions have been adjusted to match the expected CSV headers from the uploaded file.
    sql_query = """
    SELECT
        'A' AS MAINT,
        TRIM('XTP') AS SOURCEID,
        TRIM('5052026') AS DISTRIID,
        TRIM('LSH') AS DISTRISHOR,
        TRIM('5052026') AS SOLDID,
        'Lim Siang Huat Pte L)' AS SOLDIDN,
        RCT.SHIP_TO_SITE_USE_ID AS SiteCode,
        REPLACE(TRIM(hp.party_name), ',', ' ') AS CustomerName,
        CASE
            WHEN hp.party_name LIKE 'ABR Holding%' THEN '9620002'
            WHEN hp.party_name LIKE 'Kelvin%' THEN '9620002'
            WHEN hp.party_name LIKE 'Commonwealth Kokubu%' THEN '9620003'
            WHEN hp.party_name LIKE 'Commonwealth Concepts%' THEN '9620002'
            WHEN hp.party_name LIKE 'JP%' THEN '9620005'
            WHEN hp.party_name LIKE 'Aston%' THEN '9620007'
            WHEN hp.party_name LIKE 'Kenny%' THEN '9620009'
            WHEN hp.party_name LIKE 'Singapore Saizeriya%' THEN '9620006'
            WHEN hp.party_name LIKE 'Hanis%' THEN '9620011'
            WHEN hp.party_name LIKE 'Nando%' THEN '34295659'
            WHEN hp.party_name LIKE '2nd Kitchen%' THEN '9650033'
            WHEN hp.party_name LIKE 'Alpha Gourmet%' THEN '9650035'
            WHEN hp.party_name LIKE 'Arnold%' THEN '9620004'
            WHEN hp.party_name LIKE 'Aspac%' THEN '2575827'
            WHEN hp.party_name LIKE 'Biz IQ%' THEN '9650033'
            WHEN hp.party_name LIKE 'Charis%' THEN '9650034'
            WHEN hp.party_name LIKE 'Finvictus%' THEN '9650033'
            WHEN hp.party_name LIKE 'Hungry BBQ%' THEN '9650033'
            WHEN hp.party_name LIKE 'KG Catering%' THEN '9650035'
            WHEN hp.party_name LIKE 'Mango Entertainment%' THEN '1364114'
            WHEN hp.party_name LIKE 'Mayson%' THEN '9650034'
            WHEN hp.party_name LIKE 'Prive Lifestyle%' THEN '9650033'
            WHEN hp.party_name LIKE 'Bachmann%' THEN '9610008'
            WHEN hp.party_name LIKE '%Beeworks%' THEN '9650017'
            WHEN hp.party_name LIKE 'Cafe Zagu%' THEN '9650034'
            WHEN hp.party_name LIKE 'Chihanbao%' THEN '9650033'
            WHEN hp.party_name LIKE 'Continental Delight %' THEN '9650035'
            WHEN hp.party_name LIKE 'Create Restaurants%' THEN '9610017'
            WHEN hp.party_name LIKE 'Dian Xiao%' THEN '9650031'
            WHEN hp.party_name LIKE 'Dining Innovation%' THEN '9650034'
            WHEN hp.party_name LIKE 'Elsie%' THEN '9650035'
            WHEN hp.party_name LIKE 'Encik%' THEN '9650030'
            WHEN hp.party_name LIKE 'Fei Siong%' THEN '9650030'
            WHEN hp.party_name LIKE 'Foodoptions%' THEN '9650061'
            WHEN hp.party_name LIKE 'Four Seasons Food%' THEN '9650035'
            WHEN hp.party_name LIKE 'Golf Spot%' THEN '9650033'
            WHEN hp.party_name LIKE 'Han%' THEN '9620011'
            WHEN hp.party_name LIKE 'H Culture%' THEN '9650032'
            WHEN hp.party_name LIKE 'Four Seasons Food%' THEN '9650035'
            WHEN hp.party_name LIKE 'Hot Palette%' THEN '1670866'
            WHEN hp.party_name LIKE 'Stuff%' THEN '9650033'
            WHEN hp.party_name LIKE 'Hotel Miramar%' THEN '3172456'
            WHEN hp.party_name LIKE 'Japan Food%' THEN '9610008'
            WHEN hp.party_name LIKE 'JJ%' THEN '9650032'
            WHEN hp.party_name LIKE 'Jumbo Group%' THEN '9650063'
            WHEN hp.party_name LIKE 'Keppel Club%' THEN '9650033'
            WHEN hp.party_name LIKE 'Mount Faber%' THEN '9650033'
            WHEN hp.party_name LIKE 'Nam Kee%' THEN '9650030'
            WHEN hp.party_name LIKE 'SG International%' THEN '9650030'
            WHEN hp.party_name LIKE 'Nosh Cuisine%' THEN '9650035'
            WHEN hp.party_name LIKE 'OLS Holdin%' THEN '9650032'
            WHEN hp.party_name LIKE 'ONE Paradise%' THEN '9610011'
            WHEN hp.party_name LIKE 'Paradise Hotpot%' THEN '9610011'
            WHEN hp.party_name LIKE 'Poon Resources%' THEN '9650034'
            WHEN hp.party_name LIKE 'Pu Tien%' THEN '9610009'
            WHEN hp.party_name LIKE 'Que Pasa%' THEN '9650033'
            WHEN hp.party_name LIKE 'Redhill 85%' THEN '9650030'
            WHEN hp.party_name LIKE 'Royal Catering%' THEN '9650035'
            WHEN hp.party_name LIKE 'SFB Holdings%' THEN '1670866'
            WHEN hp.party_name LIKE 'Shokudo Heeren%' THEN '9610004'
            WHEN hp.party_name LIKE 'Siloso Beach%' THEN '9650033'
            WHEN hp.party_name LIKE 'Singapore Fast Food%' THEN '9650024'
            WHEN hp.party_name LIKE 'Smorgasbord International%' THEN '3172456'
            WHEN hp.party_name LIKE 'Sushiro GH%' THEN '9650034'
            WHEN hp.party_name LIKE 'Swee Choon Dim%' THEN '9650032'
            WHEN hp.party_name LIKE 'Texas Chicke%' THEN '9650060'
            WHEN hp.party_name LIKE 'THE HAINAN%' THEN '9650032'
            WHEN hp.party_name LIKE 'Thm%' THEN '9650036'
            WHEN hp.party_name LIKE 'Triple J Food%' THEN '9620007'
            WHEN hp.party_name LIKE 'Vista F%' THEN '9650063'
            WHEN hp.party_name LIKE 'Wok Master%' THEN '9650034'
            WHEN hp.party_name LIKE 'Zensho%' THEN '1670865'
            WHEN hp.party_name LIKE 'Zingrill%' THEN '9610002'
            WHEN hp.party_name LIKE 'Cathay Cineplexes %' THEN '1364114'
            WHEN hp.party_name LIKE 'Mandai Park%' THEN '1364114'
            WHEN hp.party_name LIKE 'Resorts World%' THEN '1364114'
            WHEN hp.party_name LIKE 'RW Cruis%' THEN '2575835'
            WHEN hp.party_name LIKE 'Shaw Concessions%' THEN '1364114'
            WHEN hp.party_name LIKE 'Singapore Zoo%' THEN '1364114'
            WHEN hp.party_name LIKE 'Tamerton%' THEN '2575835'
            WHEN hp.party_name LIKE '4Fingers%' THEN '9650020'
            WHEN hp.party_name LIKE 'Awesome 1%' THEN '9650059'
            WHEN hp.party_name LIKE 'Eatzi%' THEN '9620005'
            WHEN hp.party_name LIKE 'Esarn%' THEN '9650034'
            WHEN hp.party_name LIKE 'GS Restaurant%' THEN '9610001'
            WHEN hp.party_name LIKE 'Halov Singapore%' THEN '9650034'
            WHEN hp.party_name LIKE 'Hansfort%' THEN '9650035'
            WHEN hp.party_name LIKE 'Hotel Miramar%' THEN '9650035'
            WHEN hp.party_name LIKE 'Komeda Next%' THEN '9650034'
            WHEN hp.party_name LIKE 'MacKENZIE Rex%' THEN '9650034'
            WHEN hp.party_name LIKE 'Majaa%' THEN '9650034'
            WHEN hp.party_name LIKE 'Mum%' THEN '9650036'
            WHEN hp.party_name LIKE 'New Rex%' THEN '9650034'
            WHEN hp.party_name LIKE 'Pro*3%' THEN '3172456'
            WHEN hp.party_name LIKE 'SFB Holdings%' THEN '9650034'
            WHEN hp.party_name LIKE 'Siart%' THEN '9650034'
            WHEN hp.party_name LIKE 'Sum Kee Food%' THEN '9650032'
            WHEN hp.party_name LIKE '219 Food And Beverage%' THEN '9650033'
            WHEN hp.party_name LIKE 'Golden Village%' THEN '1364114'
            WHEN hp.party_name LIKE 'Tangs N%' THEN '9650030'
            WHEN hp.party_name LIKE 'Precious%' THEN '9650032'
            WHEN hp.party_name LIKE 'Seagreen%' THEN '9650032'
            WHEN hp.party_name LIKE 'Singapore Super Hi Dining%' THEN '9650032'
            WHEN hp.party_name LIKE 'AAPC%' THEN '9650033'
            WHEN hp.party_name LIKE 'Cajun On%' THEN '9650033'
            WHEN hp.party_name LIKE 'Cash Customer%' THEN '9650033'
            WHEN hp.party_name LIKE 'Cash Online%' THEN '9650033'
            WHEN hp.party_name LIKE 'Century Artisan%' THEN '9650033'
            WHEN hp.party_name LIKE 'Commonwealth Culinary%' THEN '9650033'
            WHEN hp.party_name LIKE 'Da Paolo%' THEN '9650033'
            WHEN hp.party_name LIKE 'Dapoer%' THEN '9650033'
            WHEN hp.party_name LIKE 'Deli In the Park%' THEN '9650033'
            WHEN hp.party_name LIKE 'Mangiatutto%' THEN '9650033'
            WHEN hp.party_name LIKE 'Quasont%' THEN '9650033'
            WHEN hp.party_name LIKE 'Shashlik%' THEN '9650033'
            WHEN hp.party_name LIKE 'White Tangerine%' THEN '9650033'
            WHEN hp.party_name LIKE 'Dashmesh%' THEN '9650034'
            WHEN hp.party_name LIKE 'Triple Y%' THEN '9650033'
            WHEN hp.party_name LIKE 'BidFood%' THEN '9650033'
            WHEN hp.party_name LIKE 'Delizio%' THEN '9650035'
            WHEN hp.party_name LIKE 'HILTON%' THEN '9650035'
            WHEN hp.party_name LIKE 'Kaizan%' THEN '9650034'
            WHEN hp.party_name LIKE 'SL Foods%' THEN '9650035'
            WHEN hp.party_name LIKE 'SSA Culinary%' THEN '9650035'
            WHEN hp.party_name LIKE 'Toledo%' THEN '9650034'
            WHEN hp.party_name LIKE 'Yat Yuen Hong%' THEN '9650033'
            WHEN hp.party_name LIKE 'Yishun Community%' THEN '9650033'
            WHEN hp.party_name LIKE 'Yong Wen%' THEN '9650035'
            WHEN hp.party_name LIKE 'FR KITCHEN%' THEN '3172456'
            WHEN hp.party_name LIKE 'RM Food%' THEN '3172456'
            WHEN hp.party_name LIKE 'BBZ Design%' THEN '9610004'
            WHEN hp.party_name LIKE 'The Food Theory%' THEN '9610004'
            WHEN hp.party_name LIKE 'Tung Lok%' THEN '9610006'
            WHEN hp.party_name LIKE 'All Best%' THEN '9620002'
            WHEN hp.party_name LIKE 'First Food%' THEN '9650032'
            WHEN hp.party_name LIKE 'Flavor Treasury%' THEN '9650032'
            WHEN hp.party_name LIKE 'Goodturn%' THEN '9650032'
            WHEN hp.party_name LIKE 'Orchard Grand Court%' THEN '9650032'
            WHEN hp.party_name LIKE 'Soon%' THEN '9650032'
            WHEN hp.party_name LIKE 'Swee Choon%' THEN '9650032'
            WHEN hp.party_name LIKE 'Wan Jia Yi%' THEN '9650032'
            WHEN hp.party_name LIKE 'Alice Boulangerie%' THEN '9650033'
            WHEN hp.party_name LIKE 'Annabella Patisserie%' THEN '9650033'
            WHEN hp.party_name LIKE 'Bread Butter Jam%' THEN '9650033'
            WHEN hp.party_name LIKE 'Chops Holding%' THEN '9650033'
            WHEN hp.party_name LIKE 'Creme Works%' THEN '9650033'
            WHEN hp.party_name LIKE 'Ikano%' THEN '9650033'
            WHEN hp.party_name LIKE 'Ismail Rawi%' THEN '9650033'
            WHEN hp.party_name LIKE 'Jolly Bake%' THEN '9650033'
            WHEN hp.party_name LIKE 'Little Island Brewing%' THEN '9650033'
            WHEN hp.party_name LIKE 'Massive Cravings%' THEN '9650033'
            WHEN hp.party_name LIKE 'OLE!%' THEN '9650033'
            WHEN hp.party_name LIKE 'Stamford%' THEN '3172456'
            WHEN hp.party_name LIKE 'Canton Paradise%' THEN '9610011'
            WHEN hp.party_name LIKE 'Bao Shi%' THEN '9560032'
            WHEN hp.party_name LIKE 'Convivial%' THEN '9650033'
            WHEN hp.party_name LIKE 'Four Leaves%' THEN '9650033'
            WHEN hp.party_name LIKE 'Grain%' THEN '9650033'
            WHEN hp.party_name LIKE 'Jars Ventures%' THEN '9650033'
            WHEN hp.party_name LIKE 'MOD%' THEN '9650033'
            WHEN hp.party_name LIKE 'Pullman%' THEN '9650033'
            WHEN hp.party_name LIKE 'Supergreen%' THEN '9650033'
            WHEN hp.party_name LIKE 'Timbre+%' THEN '9650033'
            WHEN hp.party_name LIKE 'Alexandra Hospital%' THEN '9650034'
            WHEN hp.party_name LIKE 'Creative Food%' THEN '9650034'
            WHEN hp.party_name LIKE 'Joleen See%' THEN '9650034'
            WHEN hp.party_name LIKE 'Plaza Premium%' THEN '9650034'
            WHEN hp.party_name LIKE 'Sampanman JP%' THEN '9650034'
            WHEN hp.party_name LIKE 'Select Services%' THEN '9650034'
            WHEN hp.party_name LIKE 'The Ultimate%' THEN '9650034'
            WHEN hp.party_name LIKE 'Fassler Gourment%' THEN '9650035'
            WHEN hp.party_name LIKE 'Furama RiverFront%' THEN '9650035'
            WHEN hp.party_name LIKE 'Just Acia%' THEN '9650035'
            WHEN hp.party_name LIKE 'Ya Ge%' THEN '9650035'
            WHEN hp.party_name LIKE 'Brenrich%' THEN '9650061'
            WHEN hp.party_name LIKE 'SK Cafe%' THEN '9650033'
            WHEN hp.party_name LIKE 'Skyfall Dock%' THEN '9650033'
            WHEN hp.party_name LIKE 'The Mind%' THEN '9650033'
            WHEN hp.party_name LIKE 'The Tyche%' THEN '9650033'
            WHEN hp.party_name LIKE 'Timbre+Hawkers%' THEN '9650033'
            WHEN hp.party_name LIKE 'Tims Restaurant%' THEN '9650033'
            WHEN hp.party_name LIKE 'Wine Trade Asia%' THEN '9650033'
            WHEN hp.party_name LIKE 'Benedict%' THEN '9650034'
            WHEN hp.party_name LIKE 'Chwm%' THEN '9650034'
            WHEN hp.party_name LIKE 'De-Prospero%' THEN '9650034'
            WHEN hp.party_name LIKE 'Fika Swedish%' THEN '9650034'
            WHEN hp.party_name LIKE 'FoodTech%' THEN '9650034'
            WHEN hp.party_name LIKE 'Onui%' THEN '9650034'
            WHEN hp.party_name LIKE 'Penang Heritage%' THEN '9650034'
            WHEN hp.party_name LIKE 'Rasa Istimewa%' THEN '9650034'
            WHEN hp.party_name LIKE 'Sampanman JWL%' THEN '9650034'
            WHEN hp.party_name LIKE 'Tana Development%' THEN '9650034'
            WHEN hp.party_name LIKE 'TEPPAN MASTER%' THEN '9650034'
            WHEN hp.party_name LIKE 'Vivian Peh%' THEN '9650034'
            WHEN hp.party_name LIKE 'Alexandra Health%' THEN '9650033'
            WHEN hp.party_name LIKE 'Beach Road Hotel%' THEN '9650033'
            WHEN hp.party_name LIKE 'Brinda%' THEN '9650034'
            WHEN hp.party_name LIKE 'Chilli Api%' THEN '9650035'
            WHEN hp.party_name LIKE 'Chin Mee%' THEN '9650035'
            WHEN hp.party_name LIKE 'First Cuisine%' THEN '9650035'
            WHEN hp.party_name LIKE 'French Food%' THEN '9650033'
            WHEN hp.party_name LIKE 'GOLDEN FOODLAND%' THEN '9650035'
            WHEN hp.party_name LIKE 'SATS PPG%' THEN '9650035'
            WHEN hp.party_name LIKE 'Shahi Foods%' THEN '9650034'
            WHEN hp.party_name LIKE 'Singapore General Hospital%' THEN '9650035'
            WHEN hp.party_name LIKE 'Sunlife Wina%' THEN '9650035'
            WHEN hp.party_name LIKE 'Team Kitchen%' THEN '9650035'
            WHEN hp.party_name LIKE 'WoodlandsHealth%' THEN '9650033'
            WHEN hp.party_name LIKE 'Wowz%' THEN '1364114'
            WHEN hp.party_name LIKE 'Shaw Service%' THEN '1364114'
            WHEN hp.party_name LIKE 'Niwa%' THEN '9610019'
            WHEN hp.party_name LIKE 'Domino%' THEN '9650013'
            WHEN hp.party_name LIKE 'Tian Tian%' THEN '9650035'
            WHEN hp.party_name LIKE 'Bread Kingdom%' THEN '9650033'
            WHEN hp.party_name LIKE 'Cafe Connection%' THEN '9650033'
            WHEN hp.party_name LIKE 'Carnivore Brazilian%' THEN '9650033'
            WHEN hp.party_name LIKE 'Chinese Swimming%' THEN '9650033'
            WHEN hp.party_name LIKE 'Colbar Cafe%' THEN '9650033'
            WHEN hp.party_name LIKE 'Cucina 17 Supplies%' THEN '9650033'
            WHEN hp.party_name LIKE 'Js Cafe%' THEN '9650033'
            WHEN hp.party_name LIKE 'Patisserie%' THEN '9650033'
            WHEN hp.party_name LIKE 'Refuel Cafe%' THEN '9650033'
            WHEN hp.party_name LIKE 'Trinity%' THEN '9650033'
            WHEN hp.party_name LIKE 'YWCA%' THEN '9650033'
            WHEN hp.party_name LIKE 'Enak Selera%' THEN '9650034'
            WHEN hp.party_name LIKE 'Jai Thai%' THEN '9650034'
            WHEN hp.party_name LIKE 'Maxwyn%' THEN '9650034'
            WHEN hp.party_name LIKE 'TENKAICHI%' THEN '9650034'
            WHEN hp.party_name LIKE 'Awfully Chocolate%' THEN '9650033'
            WHEN hp.party_name LIKE 'Bencool LA%' THEN '9650034'
            WHEN hp.party_name LIKE 'Chrisna Jenio%' THEN '9650033'
            WHEN hp.party_name LIKE 'Eatz Catering%' THEN '9650035'
            WHEN hp.party_name LIKE 'NTUC FoodFare%' THEN '9650035'
            WHEN hp.party_name LIKE 'Satoyu%' THEN '9650033'
            WHEN hp.party_name LIKE 'Tjing Sin%' THEN '9650034'
            WHEN hp.party_name LIKE 'TLG Catering%' THEN '9650035'
            WHEN hp.party_name LIKE 'Sure Food%' THEN '9610007'
            WHEN hp.party_name LIKE 'Foodgnostic%' THEN '9650034'
            WHEN hp.party_name LIKE 'On%' THEN '9650035'
            WHEN hp.party_name LIKE 'RE%' THEN '9610005'
            ELSE ' '
        END AS HUST6,
        TRIM(reps.name) AS smcode,
        hl.postal_code AS POSTALCODE,
        '                    ' AS POSTALCODE2, -- Retained as spaces as per original SQL and uploaded CSV
        'A' AS ST,
        TO_CHAR(RCT.LAST_UPDATE_DATE, 'YYYYMMDD') AS INVOICED, -- Renamed to match uploaded CSV header
        REPLACE(TRIM(HL.ADDRESS1), ',', '') AS ADDR,
        '' AS ADDR2, -- Retained as empty string
        '' AS ADDR3, -- Retained as empty string
        '' AS A, -- Mapped ADDR4 to 'A' as per uploaded CSV
        hl.postal_code AS POSTALCODE_DUP, -- Alias for the second postal code column
        '99' AS DU, -- Renamed to match uploaded CSV header
        'NP' AS KEYACCT,
        'SG23' AS SALESORG,
        '2' AS DIST, -- Renamed to match uploaded CSV header
        CASE
            WHEN hp.party_name LIKE 'ABR Holding%' THEN '060101'
            WHEN hp.party_name LIKE 'Kelvin%' THEN '060101'
            WHEN hp.party_name LIKE 'Commonwealth Kokubu%' THEN '060101'
            WHEN hp.party_name LIKE 'Commonwealth Concepts%' THEN '060101'
            WHEN hp.party_name LIKE 'JP%' THEN '060101'
            WHEN hp.party_name LIKE 'Aston%' THEN '060101'
            WHEN hp.party_name LIKE 'Kenny%' THEN '060101'
            WHEN hp.party_name LIKE 'Singapore Saizeriya %' THEN '060101'
            WHEN hp.party_name LIKE 'Hanis %' THEN '060101'
            WHEN hp.party_name LIKE 'Nando %' THEN '060101'
            WHEN hp.party_name LIKE '2nd Kitchen%' THEN '060101'
            WHEN hp.party_name LIKE 'Alpha Gourmet%' THEN '060101'
            WHEN hp.party_name LIKE 'Arnold%' THEN '060203'
            WHEN hp.party_name LIKE 'Aspac%' THEN '060203'
            WHEN hp.party_name LIKE 'Bachmann%' THEN '060102'
            WHEN hp.party_name LIKE '%Beeworks%' THEN '060203'
            WHEN hp.party_name LIKE 'Cafe Zagu%' THEN '060102'
            WHEN hp.party_name LIKE 'Chihanbao%' THEN '060101'
            WHEN hp.party_name LIKE 'Convivial%' THEN '060101'
            WHEN hp.party_name LIKE 'Four Leaves%' THEN '060101'
            WHEN hp.party_name LIKE 'Grain%' THEN '060101'
            WHEN hp.party_name LIKE 'Jars%' THEN '060101'
            WHEN hp.party_name LIKE 'MOD%' THEN '060101'
            WHEN hp.party_name LIKE 'Pullman%' THEN '060101'
            WHEN hp.party_name LIKE 'Supergreen%' THEN '060101'
            WHEN hp.party_name LIKE 'Timbre+%' THEN '060101'
            WHEN hp.party_name LIKE 'Alexandra Hospital%' THEN '060102'
            WHEN hp.party_name LIKE 'Creative Food%' THEN '060102'
            WHEN hp.party_name LIKE 'Joleen See%' THEN '060102'
            WHEN hp.party_name LIKE 'Plaza Premium Lounge%' THEN '060102'
            WHEN hp.party_name LIKE 'Sampanman JP%' THEN '060102'
            WHEN hp.party_name LIKE 'Select Services%' THEN '060102'
            WHEN hp.party_name LIKE 'The Ultimate%' THEN '060102'
            WHEN hp.party_name LIKE 'Canton Paradise%' THEN '060103'
            WHEN hp.party_name LIKE 'Bao Shi%' THEN '060103'
            WHEN hp.party_name LIKE 'Furama RiverFront%' THEN '060103'
            WHEN hp.party_name LIKE 'Just Acia%' THEN '060103'
            WHEN hp.party_name LIKE 'Ya Ge%' THEN '060103'
            WHEN hp.party_name LIKE 'Brenrich%' THEN '060203'
            WHEN hp.party_name LIKE 'Stamford%' THEN '060607'
            WHEN hp.party_name LIKE 'Fassler%' THEN '060607'
            WHEN hp.party_name LIKE 'Continental Delight%' THEN '060607'
            WHEN hp.party_name LIKE 'Create Restaurants%' THEN '060102'
            WHEN hp.party_name LIKE 'Dian Xiao%' THEN '060103'
            WHEN hp.party_name LIKE 'Dining Innovation%' THEN '060102'
            WHEN hp.party_name LIKE 'Biz IQ%' THEN '060101'
            WHEN hp.party_name LIKE 'Charis%' THEN '060102'
            WHEN hp.party_name LIKE 'Finvictus%' THEN '060101'
            WHEN hp.party_name LIKE 'Hungry BBQ%' THEN '060101'
            WHEN hp.party_name LIKE 'KG Catering%' THEN '060607'
            WHEN hp.party_name LIKE 'Mango Entertainment%' THEN '060502'
            WHEN hp.party_name LIKE 'Mayson%' THEN '060102'
            WHEN hp.party_name LIKE 'Prive Lifestyle%' THEN '060101'
            WHEN hp.party_name LIKE 'Eatzi%' THEN '060101'
            WHEN hp.party_name LIKE 'Elsie%' THEN '060607'
            WHEN hp.party_name LIKE 'Encik%' THEN '060203'
            WHEN hp.party_name LIKE 'Fei Siong%' THEN '060203'
            WHEN hp.party_name LIKE 'Foodoptions%' THEN '060203'
            WHEN hp.party_name LIKE 'Four Seasons Food%' THEN '060607'
            WHEN hp.party_name LIKE 'Golf Spot%' THEN '060101'
            WHEN hp.party_name LIKE 'Han%' THEN '060101'
            WHEN hp.party_name LIKE 'H Culture%' THEN '060103'
            WHEN hp.party_name LIKE 'Hot Palette%' THEN '060203'
            WHEN hp.party_name LIKE 'Stuff%' THEN '060101'
            WHEN hp.party_name LIKE 'Hotel Miramar%' THEN '060607'
            WHEN hp.party_name LIKE 'Japan Food%' THEN '060102'
            WHEN hp.party_name LIKE 'JJ%' THEN '060103'
            WHEN hp.party_name LIKE 'Jumbo Group%' THEN '060103'
            WHEN hp.party_name LIKE 'Keppel Club%' THEN '060101'
            WHEN hp.party_name LIKE 'Mount Faber%' THEN '060101'
            WHEN hp.party_name LIKE 'Nam Kee%' THEN '060203'
            WHEN hp.party_name LIKE 'SG International%' THEN '060203'
            WHEN hp.party_name LIKE 'Nosh Cuisine%' THEN '060607'
            WHEN hp.party_name LIKE 'OLS Holdin%' THEN '060103'
            WHEN hp.party_name LIKE 'ONE Paradise%' THEN '060103'
            WHEN hp.party_name LIKE 'Paradise Hotpot%' THEN '060103'
            WHEN hp.party_name LIKE 'Poon Resources%' THEN '060102'
            WHEN hp.party_name LIKE 'Pu Tien%' THEN '060103'
            WHEN hp.party_name LIKE 'Que Pasa%' THEN '060101'
            WHEN hp.party_name LIKE 'Redhill 85%' THEN '060203'
            WHEN hp.party_name LIKE 'Royal Catering%' THEN '060607'
            WHEN hp.party_name LIKE 'Sure Food%' THEN '060103'
            WHEN hp.party_name LIKE 'SFB Holdings%' THEN '060203'
            WHEN hp.party_name LIKE 'Shokudo Heeren%' THEN '060102'
            WHEN hp.party_name LIKE 'Siloso Beach%' THEN '060101'
            WHEN hp.party_name LIKE 'Singapore Fast Food%' THEN '060203'
            WHEN hp.party_name LIKE 'Smorgasbord International%' THEN '060607'
            WHEN hp.party_name LIKE 'Sum Kee%' THEN '060103'
            WHEN hp.party_name LIKE 'Sushiro GH%' THEN '060102'
            WHEN hp.party_name LIKE 'Swee Choon Dim%' THEN '060103'
            WHEN hp.party_name LIKE 'Texas Chicke%' THEN '060203'
            WHEN hp.party_name LIKE 'THE HAINAN%' THEN '060103'
            WHEN hp.party_name LIKE 'Thm%' THEN '060607'
            WHEN hp.party_name LIKE 'Triple J Food%' THEN '060101'
            WHEN hp.party_name LIKE 'Vista F%' THEN '060103'
            WHEN hp.party_name LIKE 'Wok Master%' THEN '060102'
            WHEN hp.party_name LIKE 'Zensho%' THEN '060203'
            WHEN hp.party_name LIKE 'Zingrill%' THEN '060102'
            WHEN hp.party_name LIKE 'Cathay Cineplexes%' THEN '060502'
            WHEN hp.party_name LIKE 'Mandai Park%' THEN '060502'
            WHEN hp.party_name LIKE 'Resorts World%' THEN '060502'
            WHEN hp.party_name LIKE 'RW Cruis%' THEN '060502'
            WHEN hp.party_name LIKE 'Shaw Concessions%' THEN '060502'
            WHEN hp.party_name LIKE 'Singapore Zoo%' THEN '060502'
            WHEN hp.party_name LIKE 'Tamerton%' THEN '060502'
            WHEN hp.party_name LIKE '4Fingers%' THEN '060203'
            WHEN hp.party_name LIKE '219 Food And Beverage%' THEN '060101'
            WHEN hp.party_name LIKE 'AAPC%' THEN '060101'
            WHEN hp.party_name LIKE 'Alpha Gourment%' THEN '060607'
            WHEN hp.party_name LIKE 'Awesome 1%' THEN '060203'
            WHEN hp.party_name LIKE 'Cajun On%' THEN '060101'
            WHEN hp.party_name LIKE 'Cash Customer%' THEN '060101'
            WHEN hp.party_name LIKE 'Cash Online%' THEN '060101'
            WHEN hp.party_name LIKE 'Century%' THEN '060101'
            WHEN hp.party_name LIKE 'Hotel Miramar%' THEN '9650035'
            WHEN hp.party_name LIKE 'HILTON Singapore%' THEN '9650035'
            WHEN hp.party_name LIKE 'Commonwealth Culinary%' THEN '060101'
            WHEN hp.party_name LIKE 'Da Paolo%' THEN '060101'
            WHEN hp.party_name LIKE 'Dapoer Peg%' THEN '060101'
            WHEN hp.party_name LIKE 'Deli In the Park%' THEN '060101'
            WHEN hp.party_name LIKE 'Mangiatutto%' THEN '060101'
            WHEN hp.party_name LIKE 'Nando%' THEN '060101'
            WHEN hp.party_name LIKE 'Quasont%' THEN '060101'
            WHEN hp.party_name LIKE 'Shashlik%' THEN '060101'
            WHEN hp.party_name LIKE 'White Tangerine%' THEN '060101'
            WHEN hp.party_name LIKE 'Dashmesh%' THEN '060102'
            WHEN hp.party_name LIKE 'Esarn Thai%' THEN '060102'
            WHEN hp.party_name LIKE 'GS Restaurants%' THEN '060102'
            WHEN hp.party_name LIKE 'Halov%' THEN '060102'
            WHEN hp.party_name LIKE 'Komeda%' THEN '060102'
            WHEN hp.party_name LIKE 'MacKENZIE Rex%' THEN '060102'
            WHEN hp.party_name LIKE 'Majaa%' THEN '060102'
            WHEN hp.party_name LIKE 'New Rex%' THEN '060102'
            WHEN hp.party_name LIKE 'SFB Holdings%' THEN '060102'
            WHEN hp.party_name LIKE 'Triple Y%' THEN '060102'
            WHEN hp.party_name LIKE 'Precious%' THEN '060103'
            WHEN hp.party_name LIKE 'Seagreen%' THEN '060103'
            WHEN hp.party_name LIKE 'Siart%' THEN '060103'
            WHEN hp.party_name LIKE 'Singapore Super Hi Dining%' THEN '060103'
            WHEN hp.party_name LIKE 'Tangs N%' THEN '060203'
            WHEN hp.party_name LIKE 'Golden Village%' THEN '060502'
            WHEN hp.party_name LIKE 'Delizio%' THEN '060607'
            WHEN hp.party_name LIKE 'Hansfort%' THEN '060607'
            WHEN hp.party_name LIKE 'Kaizan%' THEN '060102'
            WHEN hp.party_name LIKE 'Mum%' THEN '060607'
            WHEN hp.party_name LIKE 'Pro*3%' THEN '060607'
            WHEN hp.party_name LIKE 'SL Foods%' THEN '060607'
            WHEN hp.party_name LIKE 'SSA Culinary%' THEN '060607'
            WHEN hp.party_name LIKE 'Toledo%' THEN '060102'
            WHEN hp.party_name LIKE 'Yat Yuen Hong%' THEN '060101'
            WHEN hp.party_name LIKE 'Yishun Community%' THEN '060101'
            WHEN hp.party_name LIKE 'Yong Wen%' THEN '060607'
            WHEN hp.party_name LIKE 'All Best%' THEN '060101'
            WHEN hp.party_name LIKE 'Alice Boulangerie%' THEN '060101'
            WHEN hp.party_name LIKE 'Annabella%' THEN '060101'
            WHEN hp.party_name LIKE 'Bread Butter Jam%' THEN '060101'
            WHEN hp.party_name LIKE 'Chops Holding%' THEN '060101'
            WHEN hp.party_name LIKE 'Creme Works%' THEN '060101'
            WHEN hp.party_name LIKE 'Ikano%' THEN '060101'
            WHEN hp.party_name LIKE 'Ismail Rawi%' THEN '060101'
            WHEN hp.party_name LIKE 'Jolly Bake%' THEN '060101'
            WHEN hp.party_name LIKE 'Little Island%' THEN '060101'
            WHEN hp.party_name LIKE 'Massive Cravings%' THEN '060101'
            WHEN hp.party_name LIKE 'OLE!%' THEN '060101'
            WHEN hp.party_name LIKE 'SK Cafe%' THEN '060101'
            WHEN hp.party_name LIKE 'Skyfall Dock%' THEN '060101'
            WHEN hp.party_name LIKE 'The Mind%' THEN '060101'
            WHEN hp.party_name LIKE 'The Tyche%' THEN '060101'
            WHEN hp.party_name LIKE 'Timbre+Hawkers%' THEN '060101'
            WHEN hp.party_name LIKE 'Tims Restaurant%' THEN '060101'
            WHEN hp.party_name LIKE 'Wine Trade Asia%' THEN '060101'
            WHEN hp.party_name LIKE 'BBZ Design%' THEN '060102'
            WHEN hp.party_name LIKE 'The Food Theory%' THEN '060102'
            WHEN hp.party_name LIKE 'Benedict Tan%' THEN '060102'
            WHEN hp.party_name LIKE 'Chwm%' THEN '060102'
            WHEN hp.party_name LIKE 'De-Prospero%' THEN '060102'
            WHEN hp.party_name LIKE 'Fika Swedish%' THEN '060102'
            WHEN hp.party_name LIKE 'FoodTech%' THEN '060102'
            WHEN hp.party_name LIKE 'Onui%' THEN '060102'
            WHEN hp.party_name LIKE 'Penang Heritage%' THEN '060102'
            WHEN hp.party_name LIKE 'Rasa Istimewa%' THEN '060102'
            WHEN hp.party_name LIKE 'Sampanman%' THEN '060102'
            WHEN hp.party_name LIKE 'Tana Development%' THEN '060102'
            WHEN hp.party_name LIKE 'TEPPAN MASTER%' THEN '060102'
            WHEN hp.party_name LIKE 'Vivian Peh%' THEN '060102'
            WHEN hp.party_name LIKE 'First Food COY%' THEN '060103'
            WHEN hp.party_name LIKE 'Flavor Treasury%' THEN '060103'
            WHEN hp.party_name LIKE 'Goodturn Dining%' THEN '060103'
            WHEN hp.party_name LIKE 'Orchard Grand Court%' THEN '060103'
            WHEN hp.party_name LIKE 'Soon%' THEN '060103'
            WHEN hp.party_name LIKE 'Swee Choon Kitchen%' THEN '060103'
            WHEN hp.party_name LIKE 'Wan Jia Yi Zu%' THEN '060103'
            WHEN hp.party_name LIKE 'FR KITCHEN%' THEN '060607'
            WHEN hp.party_name LIKE 'RM Food%' THEN '060607'
            WHEN hp.party_name LIKE 'Tung Lok%' THEN '060607'
            WHEN hp.party_name LIKE 'Alexandra Health%' THEN '060101'
            WHEN hp.party_name LIKE 'Beach Road Hotel%' THEN '060101'
            WHEN hp.party_name LIKE 'Bidfood%' THEN '060101'
            WHEN hp.party_name LIKE 'Brinda%' THEN '060102'
            WHEN hp.party_name LIKE 'Chilli Api%' THEN '060607'
            WHEN hp.party_name LIKE 'Chin Mee%' THEN '060607'
            WHEN hp.party_name LIKE 'First Cuisine%' THEN '060607'
            WHEN hp.party_name LIKE 'French Food%' THEN '060101'
            WHEN hp.party_name LIKE 'GOLDEN FOODLAND%' THEN '060607'
            WHEN hp.party_name LIKE 'SATS PPG%' THEN '060607'
            WHEN hp.party_name LIKE 'Shahi Foods%' THEN '060102'
            WHEN hp.party_name LIKE 'Singapore General Hospital%' THEN '060607'
            WHEN hp.party_name LIKE 'Sunlife Wina%' THEN '060607'
            WHEN hp.party_name LIKE 'Team Kitchen%' THEN '060607'
            WHEN hp.party_name LIKE 'WoodlandsHealth%' THEN '060101'
            WHEN hp.party_name LIKE 'Wowz%' THEN '060502'
            WHEN hp.party_name LIKE 'Bread Kingdom%' THEN '060101'
            WHEN hp.party_name LIKE 'Cafe Connection%' THEN '060101'
            WHEN hp.party_name LIKE 'Carnivore%' THEN '060101'
            WHEN hp.party_name LIKE 'Chinese Swimming Club%' THEN '060101'
            WHEN hp.party_name LIKE 'Colbar Cafe%' THEN '060101'
            WHEN hp.party_name LIKE 'Cucina 17%' THEN '060101'
            WHEN hp.party_name LIKE 'Js Cafe%' THEN '060101'
            WHEN hp.party_name LIKE 'Patisserie%' THEN '060101'
            WHEN hp.party_name LIKE 'Refuel Cafe%' THEN '060101'
            WHEN hp.party_name LIKE 'Trinity Christian%' THEN '060101'
            WHEN hp.party_name LIKE 'YWCA%' THEN '060101'
            WHEN hp.party_name LIKE 'Niwa%' THEN '060102'
            WHEN hp.party_name LIKE 'Enak Selera%' THEN '060102'
            WHEN hp.party_name LIKE 'Jai Thai%' THEN '060102'
            WHEN hp.party_name LIKE 'Maxwyn%' THEN '060102'
            WHEN hp.party_name LIKE 'TENKAICHI%' THEN '060102'
            WHEN hp.party_name LIKE 'Tian Tian%' THEN '060103'
            WHEN hp.party_name LIKE 'Domino%' THEN '060103'
            WHEN hp.party_name LIKE 'Shaw Service%' THEN '060502'
            WHEN hp.party_name LIKE 'Awfully Chocolate%' THEN '060101'
            WHEN hp.party_name LIKE 'Bencool LA%' THEN '060102'
            WHEN hp.party_name LIKE 'Chrisna Jenio%' THEN '060101'
            WHEN hp.party_name LIKE 'Eatz Catering%' THEN '060607'
            WHEN hp.party_name LIKE 'Foodgnostic%' THEN '060102'
            WHEN hp.party_name LIKE 'NTUC FoodFare%' THEN '060607'
            WHEN hp.party_name LIKE 'Satoyu%' THEN '060101'
            WHEN hp.party_name LIKE 'Tjing Sin%' THEN '060102'
            WHEN hp.party_name LIKE 'TLG Catering%' THEN '060607'
            WHEN hp.party_name LIKE 'On%' THEN '060607'
            WHEN hp.party_name LIKE 'RE%' THEN '060102'
            ELSE ' '
        END AS OUT1
    FROM
        oe_order_lines_all OEL,
        OE_ORDER_HEADERS_ALL OEH,
        RA_CUSTOMER_TRX_ALL RCT,
        RA_CUSTOMER_TRX_LINES_ALL rctl,
        HZ_CUST_SITE_USES_ALL HCSU,
        HZ_CUST_ACCT_SITES_ALL HCAS,
        HZ_CUST_ACCOUNTS HCA,
        HZ_PARTY_SITES HPS,
        HZ_PARTIES HP,
        HZ_LOCATIONS HL,
        ra_salesreps_all reps,
        MTL_SYSTEM_ITEMS_B MSI
    WHERE
        OEH.HEADER_ID = OEL.HEADER_ID
        AND RCT.INTERFACE_HEADER_ATTRIBUTE1 = TO_NUMBER(OEH.ORDER_NUMBER)
        AND RCTL.INTERFACE_LINE_ATTRIBUTE6 = OEL.LINE_ID
        AND RCTL.CUSTOMER_TRX_ID = RCT.CUSTOMER_TRX_ID
        AND MSI.INVENTORY_ITEM_ID = RCTL.INVENTORY_ITEM_ID
        AND reps.SALESREP_ID = OEL.SALESREP_ID
        AND HCSU.SITE_USE_ID = rct.ship_to_site_use_id
        AND HCSU.CUST_ACCT_SITE_ID = HCAS.CUST_ACCT_SITE_ID
        AND HCAS.CUST_ACCOUNT_ID = HCA.CUST_ACCOUNT_ID
        AND HPS.PARTY_SITE_ID = HCAS.PARTY_SITE_ID
        AND HCA.PARTY_ID = Hp.PARTY_ID
        AND HPS.PARTY_ID = HP.PARTY_ID
        AND HCAS.PARTY_SITE_ID = HPS.PARTY_SITE_ID
        AND HPS.LOCATION_ID = HL.LOCATION_ID
        AND MSI.SEGMENT1 LIKE 'XN%'
        AND oel.invoiced_quantity <> 0
        AND MSI.ORGANIZATION_ID = 82
        AND TRUNC(RCT.CREATION_DATE) BETWEEN TO_DATE('01-JAN-2025', 'DD-MON-YYYY') AND TO_DATE('31-DEC-2026', 'DD-MON-YYYY')
    """

    # Define the column headers to exactly match the uploaded CSV file.
    csv_headers = [
        "MAINT", "SOURCEID", "DISTRIID", "DISTRISHOR", "SOLDID", "SOLDIDN",
        "SiteCode", "CustomerName", "HUST6", "smcode", "POSTALCODE", "POSTALCODE2",
        "ST", "INVOICED", "ADDR", "ADDR2", "ADDR3", "A", "POSTALCODE",
        "DU", "KEYACCT", "SALESORG", "DIST", "OUT1"
    ]

    connection = None
    cursor = None
    try:
        # Establish database connection
        print(f"Attempting to connect to Oracle database with DSN: {dsn}...")
        connection = cx_Oracle.connect(username, password, dsn)
        cursor = connection.cursor()
        print("Database connection successful.")

        # Execute the SQL query
        print("Executing SQL query...")
        cursor.execute(sql_query)

        rows = cursor.fetchall()
        print(f"Query executed. Fetched {len(rows)} rows.")

        # Ensure the output directory exists
        output_dir = os.path.dirname(output_filename)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created directory: {output_dir}")

        # Write data to CSV
        print(f"Writing data to {output_filename}...")
        with open(output_filename, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)

            # Write header row
            csv_writer.writerow(csv_headers)

            # Write data rows
            csv_writer.writerows(rows)

        print(f"Data successfully exported to {output_filename}")

    except cx_Oracle.Error as e:
        error_obj, = e.args
        print(f"Oracle Error Code: {error_obj.code}")
        print(f"Oracle Error Message: {error_obj.message}")
        print("An error occurred during database operation.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()
            print("Cursor closed.")
        if connection:
            connection.close()
            print("Database connection closed.")

# To run this module, you can call the function directly:
if __name__ == "__main__":
     run_sql_query_to_csv()
