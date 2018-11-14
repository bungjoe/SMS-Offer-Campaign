CREATE OR REPLACE PROCEDURE CAMP_SMS_FF_INBOUND AS
    DELIVERY VARCHAR(30); DT_CURRENT TIMESTAMP;     DT_START DATE;              DT_END DATE; 
    HORUS NUMBER;         SMS_COUNT NUMBER;         DAY_COUNT NUMBER;           CSIZE NUMBER;
    HSIZE NUMBER;         CONTACT_NUM VARCHAR2(50); CONTACT_NAME VARCHAR2(150); DAY_TYPES VARCHAR2(50);
    GENDERS VARCHAR2(50); TEXT_SMS VARCHAR(180);    TIME_GROUP VARCHAR(5);      ROW_ID VARCHAR(100);        MESSAGE_CODE VARCHAR2(50);

    CURSOR CUR_MON IS
          SELECT CSM.CONTACT_NUMBER, CSM.CONTACT_NAME, CSM.GENDER FROM CAMP_SMS_MONITOR CSM
          INNER JOIN CAMP_SMS_MONITOR_DETAIL CSD ON CSM.CONTACT_NUMBER = CSD.CONTACT_NUMBER AND CSD.STATUS = 1
          WHERE CSM.FLAG_ACTIVE = 1 AND CSD.CHANNEL = 'SMS OFFER CAMPAIGN';

    CURSOR CUR_ROW IS
           SELECT ROWID FROM CAMP_SMS_AD_HOC_CRM WHERE DELIVERY_TIME >= TRUNC(DT_CURRENT)
           AND CODE_PROMO IN
               (
                    SELECT MESSAGE_CODE FROM CAMP_CFG_SMS_TEXT WHERE CAMPAIGN_ID = TO_CHAR(DT_CURRENT,'YYMM') AND SMS_TYPE ='INBOUND'
               )
           AND CUID <> 615898;
    
PROCEDURE PSTATS( ACTABLE VARCHAR2, ANPERC NUMBER DEFAULT 0.01) IS
BEGIN
    AP_PUBLIC.CORE_LOG_PKG.PSTART( 'Stat:'||UPPER(ACTABLE) );
    DBMS_STATS.GATHER_TABLE_STATS( OWNNAME => 'AP_CRM', TABNAME => ACTABLE,ESTIMATE_PERCENT => ANPERC );
    AP_PUBLIC.CORE_LOG_PKG.PEND;
END;
PROCEDURE PTRUNCATE( ACTABLE VARCHAR2) IS
BEGIN
    AP_PUBLIC.CORE_LOG_PKG.PSTART( 'Trunc:'||UPPER(ACTABLE) );
    EXECUTE IMMEDIATE 'TRUNCATE TABLE AP_CRM.'||UPPER(ACTABLE) ;
    AP_PUBLIC.CORE_LOG_PKG.PEND ;
END;
BEGIN
    AP_PUBLIC.CORE_LOG_PKG.PINIT( 'AP_CRM', 'CAMP_SMS_FF_INBOUND');
    DT_CURRENT := SYSDATE;
    DAY_TYPES := GET_DAY_TYPE_CALENDAR(TRUNC(DT_CURRENT));
    
    IF DAY_TYPES <> 'Workday' THEN
       GOTO FINISH_LINE;
    END IF;
    
    PTRUNCATE('GTT_CAMP_SMS_FF_UPDATE');
    AP_PUBLIC.CORE_LOG_PKG.PSTART('Insert into gtt_camp_sms_ff_update');
    INSERT /*+ APPEND PARALLEL(4) */ INTO GTT_CAMP_SMS_FF_UPDATE
    WITH A AS(
    SELECT DISTINCT /*+ MATERIALIZE */ TO_CHAR(EB.VALID_FROM, 'yymm') CAMPAIGN_ID, EB.CALL_TO_ACTION,
           SKP_CLIENT,                               ID_CUID,
           CA_LIMIT_FINAL_UPDATED MAX_CREDIT_AMOUNT, ANNUITY_LIMIT_FINAL_UPDATED MAX_ANNUITY,
           PRIORITY_ACTUAL PRIORITY,                 VALID_FROM,
           VALID_TO,                                 RISK_BAND RISK_GROUP,
           RBP_SEGMENT_TEMP,                         FLOOR(((INTEREST_RATE/100) * CA_LIMIT_FINAL_UPDATED * MAX_TENOR + CA_LIMIT_FINAL_UPDATED)/MAX_TENOR) + 5000 AS INSTALMENT,
           MAX_TENOR TENOR
    FROM AP_CRM.CAMP_SMS_FF_BASE EB
    WHERE 1=1
          AND EB.CAMPAIGN_ID = TO_CHAR(DT_CURRENT, 'YYMM')
          AND EB.CALL_TO_ACTION = 'INBOUND'
          AND EB.ID_CUID NOT IN (SELECT NVL(CUID,-999999999) FROM V_CAMP_BLACK_LIST)
          AND EB.ID_CUID NOT IN 
              (
                   SELECT NVL(ID_CUID,-999999999) FROM CAMP_ELIG_DAILY_CHECK 
                   WHERE TRUNC(DT_CURRENT) BETWEEN DATE_VALID_FROM AND DATE_VALID_TO AND NVL(FLAG_STILL_ELIGIBLE,'U') = 'N'
              )
    ),
    
    UPLOAD_HOSEL AS
    (
    SELECT /*+ MATERIALIZE */ FCC.SKP_CLIENT, FCC.SKF_CAMPAIGN_CLIENT, FCC.SKP_CAMPAIGN, FCC.NAME_OFFER, FCC.DATE_VALID_FROM, FCC.DATE_VALID_TO
    FROM AP_CRM.CAMP_CLIENT_AT FCC
    WHERE LOWER(FLAG_DELETED) = 'n' AND NVL(FCC.SKP_CLIENT,-99999) IN (SELECT NVL(SKP_CLIENT,'-99999') FROM A)
    )
        
        SELECT /*+ MATERIALIZE */ DISTINCT A.*,
        CASE  WHEN (CCI.NAME_FIRST LIKE 'Drs' OR CCI.NAME_FIRST LIKE 'Drs.') AND CCI.NAME_MIDDLE='XNA' THEN CCI.NAME_LAST
              WHEN (CCI.NAME_FIRST LIKE 'Drs' OR CCI.NAME_FIRST LIKE 'Drs.') AND CCI.NAME_MIDDLE<>'XNA' THEN CCI.NAME_MIDDLE
              WHEN (CCI.NAME_FIRST LIKE 'Ir' OR CCI.NAME_FIRST LIKE 'Ir.') AND CCI.NAME_MIDDLE='XNA' THEN CCI.NAME_LAST
              WHEN (CCI.NAME_FIRST LIKE 'Ir' OR CCI.NAME_FIRST LIKE 'Ir.') AND CCI.NAME_MIDDLE<>'XNA' THEN CCI.NAME_MIDDLE
              WHEN (CCI.NAME_FIRST LIKE 'H' OR CCI.NAME_FIRST LIKE 'H.') AND CCI.NAME_MIDDLE='XNA' THEN CCI.NAME_LAST
              WHEN (CCI.NAME_FIRST LIKE 'H' OR CCI.NAME_FIRST LIKE 'H.') AND CCI.NAME_MIDDLE<>'XNA' THEN CCI.NAME_MIDDLE
              WHEN (CCI.NAME_FIRST LIKE 'Hj' OR CCI.NAME_FIRST LIKE 'Hj.') AND CCI.NAME_MIDDLE='XNA' THEN CCI.NAME_LAST
              WHEN (CCI.NAME_FIRST LIKE 'Hj' OR CCI.NAME_FIRST LIKE 'Hj.') AND CCI.NAME_MIDDLE<>'XNA' THEN CCI.NAME_MIDDLE
              WHEN CCI.NAME_FIRST IN ('A', 'B', 'C', 'D', 'E', 'F',  'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
                    'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',  'X',  'Y', 'Z') AND CCI.NAME_MIDDLE='XNA' THEN CCI.NAME_LAST
              WHEN CCI.NAME_FIRST IN ('A',  'B', 'C', 'D',  'E', 'F',  'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
                    'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',  'W', 'X',  'Y', 'Z') AND CCI.NAME_MIDDLE<>'XNA' THEN CCI.NAME_MIDDLE
              WHEN CCI.NAME_FIRST IN ('A.', 'B.', 'C.', 'D.',  'E.',  'F.',  'G.', 'H.', 'I.', 'J.', 'K.', 'L.', 'M.', 'N.',
                    'O.', 'P.', 'Q.', 'R.', 'S.', 'T.', 'U.',  'V.',  'W.',  'X.', 'Y.', 'Z.') AND CCI.NAME_MIDDLE='XNA' THEN CCI.NAME_LAST
              WHEN CCI.NAME_FIRST IN ('A.', 'B.', 'C.', 'D.', 'E.',  'F.', 'G.', 'H.', 'I.', 'J.', 'K.', 'L.', 'M.', 'N.',
                    'O.', 'P.', 'Q.', 'R.', 'S.', 'T.', 'U.',  'V.', 'W.',  'X.', 'Y.', 'Z.') AND CCI.NAME_MIDDLE<>'XNA' THEN CCI.NAME_MIDDLE
        ELSE INITCAP(CCI.NAME_FIRST) END AS NAME_UPDATE,
        CASE  WHEN GENDER = 'Male' THEN 'Bpk'
              WHEN GENDER = 'Female' THEN 'Ibu'
        ELSE ' ' END AS GENDER_GROUP,
        COALESCE(CFP.PHONE1,CFP.PHONE2)PHONE_UPDATE,
        TRUNC(ROUND((A.MAX_CREDIT_AMOUNT/1000000),2),1) AS CREDIT_AMOUNT,
        CASE  WHEN A.INSTALMENT >=1000000 THEN ROUND((A.INSTALMENT/1000000),2)||'jt/bln'
              WHEN A.INSTALMENT <1000000 THEN FLOOR(A.INSTALMENT/1000)||'rb/bln'
        END AS INSTALMENT_UPDATE
        FROM A
        INNER JOIN AP_CRM.CAMP_CLIENT_IDENTITY CCI ON A.SKP_CLIENT = CCI.SKP_CLIENT
        INNER JOIN AP_CRM.V_CAMP_FINAL_PHONENUM CFP ON A.ID_CUID = CFP.ID_CUID
        WHERE NVL(A.SKP_CLIENT,-9999) IN (SELECT NVL(SKP_CLIENT,'-9999') FROM UPLOAD_HOSEL)
        AND (CFP.PHONE1 IS NOT NULL OR CFP.PHONE2 IS NOT NULL)
    ;
    AP_PUBLIC.CORE_LOG_PKG.PEND;
    COMMIT;
    PSTATS('GTT_CAMP_SMS_FF_UPDATE');
    
    AP_PUBLIC.CORE_LOG_PKG.PSTART('Flush sms record with delivery time of today if any');
    DELETE FROM AP_CRM.CAMP_SMS_AD_HOC_CRM       
    WHERE DELIVERY_TIME >= TRUNC(DT_CURRENT)
          AND CODE_PROMO = 'CRM:SMS_FF_OFFER_INBOUND'
    ;
    AP_PUBLIC.CORE_LOG_PKG.PEND;
    COMMIT;
    PSTATS('camp_sms_ad_hoc_crm');
    
    PTRUNCATE('GTT_CAMP_SMS_FF_MONTH');
    AP_PUBLIC.CORE_LOG_PKG.PSTART('Collecting smses in this month');
    INSERT /*+ APPEND PARALLEL(4) */ INTO GTT_CAMP_SMS_FF_MONTH
    SELECT CUID, CODE_PROMO, TRUNC(DELIVERY_TIME) DELIVERY_DATE
    FROM AP_CRM.CAMP_SMS_AD_HOC_CRM
    WHERE DELIVERY_TIME >= TRUNC(DT_CURRENT, 'MM')
          AND CODE_PROMO IN (SELECT MESSAGE_CODE FROM CAMP_CFG_SMS_TEXT WHERE CAMPAIGN_ID = TO_CHAR(DT_CURRENT,'YYMM') 
                             AND SMS_TYPE IN (/*'OFFER REGULAR', 'APP', */'INBOUND'/*, 'LANDING'*/))
    ;
    AP_PUBLIC.CORE_LOG_PKG.PEND;
    COMMIT;
    PSTATS('GTT_CAMP_SMS_FF_MONTH');
    
    PTRUNCATE('GTT_CAMP_SMS_COUNTS');
    AP_PUBLIC.CORE_LOG_PKG.PSTART('Collecting counts of smses');
    INSERT /*+ APPEND PARALLEL(4) */ INTO GTT_CAMP_SMS_COUNTS
    SELECT CUID, COUNT(CUID)COUNT_SMS FROM GTT_CAMP_SMS_FF_MONTH
    GROUP BY CUID;
    AP_PUBLIC.CORE_LOG_PKG.PEND;
    COMMIT;
    PSTATS('GTT_CAMP_SMS_COUNTS');
   
    PTRUNCATE('gtt_camp_sms_last');
    AP_PUBLIC.CORE_LOG_PKG.PSTART('Collecting last time of smses sent');
    INSERT /*+ APPEND PARALLEL(4) */ INTO GTT_CAMP_SMS_LAST
    SELECT CUID, MAX(DELIVERY_DATE)SEND_DATE FROM GTT_CAMP_SMS_FF_MONTH
    GROUP BY CUID;
    AP_PUBLIC.CORE_LOG_PKG.PEND;
    COMMIT;
    PSTATS('gtt_camp_sms_last');
    
    /* Get day count to spread the sms sending */
    WITH ALL_DAY AS
    (
        SELECT /*+ MATERIALZIE */ TO_CHAR(DT_CURRENT,'yymm')CAMPAIGN_ID, TRUNC(DT_CURRENT,'MM') + (LEVEL-1) AS DATE_D
        FROM DUAL
        CONNECT BY (LEVEL-1) <= 31
    ),
    T1 AS
    (
        SELECT /*+ MATERIALZIE */ TO_CHAR(DT_CURRENT,'yymm')CAMPAIGN_ID, TRUNC(DT_CURRENT,'MM') + ((LEVEL-1)*5) AS DATE_D
        FROM DUAL
        CONNECT BY ((LEVEL-1)*5) <= 31
    ),
    T2 AS
    (
        SELECT TO_CHAR(DT_CURRENT,'yymm')CAMPAIGN_ID, DATE_D DATE_START,
        CASE WHEN ROWNUM < 6 THEN DATE_D + 4 ELSE DATE_D + 5 END DATE_END,
        ROWNUM  SMS_CYCLE
        FROM T1 WHERE ROWNUM <= 6
    )
    SELECT T2.DATE_START, T2.DATE_END, T2.SMS_CYCLE, COUNT(DATE_D)
          INTO DT_START, DT_END, SMS_COUNT, DAY_COUNT
    FROM ALL_DAY
    INNER JOIN T2 ON ALL_DAY.CAMPAIGN_ID = T2.CAMPAIGN_ID AND ALL_DAY.DATE_D BETWEEN T2.DATE_START AND T2.DATE_END
    WHERE ALL_DAY.DATE_D BETWEEN TRUNC(DT_CURRENT) AND (SELECT DATE_END FROM T2 WHERE TRUNC(DT_CURRENT) BETWEEN DATE_START AND DATE_END)
    GROUP BY T2.DATE_START, T2.DATE_END, T2.SMS_CYCLE;

    SELECT COUNT(CAL.DATE_ID) INTO DAY_COUNT FROM CAMP_CALENDAR CAL 
    WHERE (CAL.DATE_ID BETWEEN TRUNC(DT_CURRENT) AND DT_END)
          AND CAL.ISHOLIDAY = 0 AND CAL.ISWEEKDAY = 1;
          
    PTRUNCATE('gtt_camp_sms_ff_text');
    AP_PUBLIC.CORE_LOG_PKG.PSTART('Collecting sms text in current cycle');
    INSERT INTO GTT_CAMP_SMS_FF_TEXT
    SELECT * FROM CAMP_CFG_SMS_TEXT
    WHERE CAMPAIGN_ID = TO_CHAR(DT_CURRENT, 'YYMM')
          AND SMS_TYPE = 'INBOUND'
          AND TO_NUMBER(SUBSTR(TEXT_TYPE, -1, 1)) = SMS_COUNT;
    COMMIT;
    AP_PUBLIC.CORE_LOG_PKG.PEND;
    COMMIT;
    PSTATS('gtt_camp_sms_ff_text');
    
    PTRUNCATE('gtt_camp_sms_list');
    AP_PUBLIC.CORE_LOG_PKG.PSTART('Building list of smses to sent');
    INSERT /*+ APPEND */ INTO GTT_CAMP_SMS_LIST
    (SKP_CLIENT,	CUID,	CONTRACT_ID,	MOBILE1,	PILOT_FLAG,	TDY_PRIORITY,	COUNT_SMS,	LAST_SMS, SMS_TEXT, MESSAGE_CODE)
    SELECT /*+ PARALLEL(4) USE_HASH(base sc sms sl) */
    BASE.SKP_CLIENT,        BASE.ID_CUID,          AX.CONTRACT_ID,                    AX.MOBILE1, 
    AX.PILOT_FLAG,          CCL.TDY_PRIORITY,      NVL(SC.COUNT_SMS,0)COUNT_SMS,      TRUNC(DT_CURRENT) - NVL(TRUNC(SL.SEND_DATE),TRUNC(DT_CURRENT)-6) LAST_SMS,
    REPLACE(REPLACE(REPLACE(TEXT_CONTENT, '[nick_name]',CASE WHEN LENGTH(NAME_UPDATE) > 10 THEN 'Nasabah Yth' ELSE  GENDER_GROUP || ' ' || NAME_UPDATE END),'[instalment]', BASE.INSTALMENT_UPDATE), '[credit_amount]', BASE.CREDIT_AMOUNT) SMS_TEXT,
    MESSAGE_CODE
    FROM GTT_CAMP_SMS_FF_UPDATE BASE
    INNER JOIN CAMP_TDY_CALL_LIST AX ON BASE.ID_CUID = AX.CUID 
    LEFT JOIN GTT_CAMP_SMS_COUNTS SC ON BASE.ID_CUID = SC.CUID
    LEFT JOIN GTT_CAMP_SMS_FF_TEXT SMS ON BASE.CALL_TO_ACTION = SMS.SMS_TYPE AND SMS.CAMPAIGN_ID = TO_CHAR(DT_CURRENT,'YYMM')
    LEFT JOIN GTT_CAMP_SMS_LAST SL ON BASE.ID_CUID = SL.CUID
    LEFT JOIN CAMP_COMPILED_LIST CCL ON BASE.SKP_CLIENT = CCL.SKP_CLIENT
    WHERE 1=1
          AND (LOWER(AX.INFO2) NOT LIKE '6.Fol%' OR LOWER(AX.INFO2) NOT LIKE '%disaster%')
          AND COALESCE(CCL.DT_COMPLAINT, CCL.DT_DWTO, CCL.DT_DWTO2) IS NULL
          AND BASE.ID_CUID NOT IN
                (
                    SELECT CUID FROM AP_CRM.CAMP_SMS_AD_HOC_CRM WHERE TRUNC(DELIVERY_TIME) BETWEEN DT_START AND DT_END
                    AND CODE_PROMO IN
                        (
                            SELECT MESSAGE_CODE FROM CAMP_CFG_SMS_TEXT WHERE CAMPAIGN_ID = TO_CHAR(DT_CURRENT,'YYMM') 
                            AND SMS_TYPE IN (/*'OFFER REGULAR', 'APP', */'INBOUND'/*, 'LANDING'*/)
                        )
                )
          AND BASE.SKP_CLIENT NOT IN (SELECT NVL(SKP_CLIENT,-99999) FROM V_CAMP_LANDING_PAGE WHERE DATE_CREATED >= TRUNC(DT_CURRENT-30)) /* Added @ October 5th 2017 */
          AND BASE.ID_CUID NOT IN
          ( /* find From Mobile App Form */
             SELECT NVL(CUID,-99999) FROM AP_CRM.V_CAMP_MOBILE_APP
          )
          AND NVL(SC.COUNT_SMS,0) < SMS_COUNT;
    AP_PUBLIC.CORE_LOG_PKG.PEND;
    COMMIT;
    PSTATS('gtt_camp_sms_list');
    
    SELECT COUNT(1) INTO CSIZE FROM AP_CRM.GTT_CAMP_SMS_LIST;
    SELECT TEXT_CONTENT, MESSAGE_CODE INTO TEXT_SMS, MESSAGE_CODE FROM AP_CRM.GTT_CAMP_SMS_FF_TEXT;
    
        IF CSIZE > 0 THEN

        SELECT CEIL(COUNT(SKP_CLIENT)/DAY_COUNT) INTO CSIZE FROM AP_CRM.GTT_CAMP_SMS_LIST;
        HSIZE := CEIL(CSIZE/10);
        AP_PUBLIC.CORE_LOG_PKG.PSTART('INS:DISTRIBUTE REG SMS');
        INSERT INTO AP_CRM.CAMP_SMS_AD_HOC_CRM
        WITH BASE AS (
            SELECT ROW_NUMBER() OVER (ORDER BY COUNT_SMS ASC, LAST_SMS DESC, TDY_PRIORITY ASC)NUMSA, OFR.* FROM AP_CRM.GTT_CAMP_SMS_LIST OFR
        )
        SELECT DISTINCT MOBILE1 PHONE, 'GINQ' COMM_TYPE, 'GI_OTHERS' COMM_SUBTYPE,
        CASE WHEN NUMSA <= HSIZE * 1 THEN TO_TIMESTAMP(TO_CHAR(DT_CURRENT,'mm/dd/yyyy') || ' 08:00:00','mm/dd/yyyy hh24:mi:ss')
             WHEN NUMSA <= HSIZE * 2 THEN TO_TIMESTAMP(TO_CHAR(DT_CURRENT,'mm/dd/yyyy') || ' 09:00:00','mm/dd/yyyy hh24:mi:ss')
             WHEN NUMSA <= HSIZE * 3 THEN TO_TIMESTAMP(TO_CHAR(DT_CURRENT,'mm/dd/yyyy') || ' 10:00:00','mm/dd/yyyy hh24:mi:ss')
             WHEN NUMSA <= HSIZE * 4 THEN TO_TIMESTAMP(TO_CHAR(DT_CURRENT,'mm/dd/yyyy') || ' 11:00:00','mm/dd/yyyy hh24:mi:ss')
             WHEN NUMSA <= HSIZE * 5 THEN TO_TIMESTAMP(TO_CHAR(DT_CURRENT,'mm/dd/yyyy') || ' 12:00:00','mm/dd/yyyy hh24:mi:ss')
             WHEN NUMSA <= HSIZE * 6 THEN TO_TIMESTAMP(TO_CHAR(DT_CURRENT,'mm/dd/yyyy') || ' 13:00:00','mm/dd/yyyy hh24:mi:ss')
             WHEN NUMSA <= HSIZE * 7 THEN TO_TIMESTAMP(TO_CHAR(DT_CURRENT,'mm/dd/yyyy') || ' 14:00:00','mm/dd/yyyy hh24:mi:ss')
             WHEN NUMSA <= HSIZE * 8 THEN TO_TIMESTAMP(TO_CHAR(DT_CURRENT,'mm/dd/yyyy') || ' 15:00:00','mm/dd/yyyy hh24:mi:ss')
             WHEN NUMSA <= HSIZE * 9 THEN TO_TIMESTAMP(TO_CHAR(DT_CURRENT,'mm/dd/yyyy') || ' 16:00:00','mm/dd/yyyy hh24:mi:ss')
             WHEN NUMSA <= HSIZE * 10 THEN TO_TIMESTAMP(TO_CHAR(DT_CURRENT,'mm/dd/yyyy') || ' 17:00:00','mm/dd/yyyy hh24:mi:ss')
        ELSE TO_TIMESTAMP(TO_CHAR(DT_CURRENT,'mm/dd/yyyy') || ' 17:59:00','mm/dd/yyyy hh24:mi:ss')
        END DELIVERY_TIME,
        CUID ID_CUID, CONTRACT_ID CONTRACT_CODE, SMS_TEXT MESSAGE, 'N' FLAG_SENT_TO_PROVDR, DT_CURRENT DTIME_INSERTED, NULL DTIME_SENT_TO_PROVDR, 'CRM' DEPARTMENT, 'DL' COMM_STATUS, 'CLS' COMM_RESULT,
        BASE.MESSAGE_CODE CODE_PROMO, NULL
        FROM BASE WHERE NUMSA <= CSIZE;
        
        AP_PUBLIC.CORE_LOG_PKG.pEnd;
        commit;
        pStats(UPPER('CAMP_SMS_AD_HOC_CRM'));        
        
        AP_PUBLIC.CORE_LOG_PKG.PSTART('INS:DISTRIBUTE CONTROL SMS');
        IF NOT (CUR_MON%ISOPEN) THEN
            OPEN CUR_MON;
        END IF;
        LOOP
        FETCH CUR_MON INTO CONTACT_NUM, CONTACT_NAME, GENDERS;
        EXIT WHEN CUR_MON%NOTFOUND;
          FOR HORUS IN 8..17
          LOOP
            DELIVERY := TO_CHAR(DT_CURRENT,'mm/dd/yyyy') || ' ' || TO_CHAR(HORUS, '09') || ':00:00';
            INSERT INTO AP_CRM.CAMP_SMS_AD_HOC_CRM VALUES (CONTACT_NUM, 'GINQ', 'GI_OTHERS', TO_TIMESTAMP(DELIVERY,'mm/dd/yyyy hh24:mi:ss'), '615898', '3608717024',
            REPLACE(REPLACE(REPLACE(TEXT_SMS,'[credit_amount]','25'),'[instalment]','625rb/bln'),'[nick_name]', CASE WHEN LENGTH(TRIM(CONTACT_NAME)) > 12 THEN 'Nasabah Yth' ELSE 'Yth. ' || CASE WHEN GENDERS = 'Male' THEN 'Bpk' WHEN GENDERS = 'Female' THEN 'Ibu' END || ' ' ||TRIM(INITCAP(CONTACT_NAME)) END),
              'N', DT_CURRENT, NULL,'CRM','DL','CLS',MESSAGE_CODE,NULL);
          END LOOP;
        HORUS := 8;
        END LOOP;
        CLOSE CUR_MON;
        
        AP_PUBLIC.CORE_LOG_PKG.pEnd;
        commit;
        pStats(UPPER('CAMP_SMS_AD_HOC_CRM'));
    
    AP_PUBLIC.CORE_LOG_PKG.pStart('UPD:TIME SETTING');
        if not (cur_row%ISOPEN) then
            open cur_row;
        end if;
        loop
        fetch cur_row into row_id;
        exit when cur_row%NOTFOUND;
         update camp_sms_ad_hoc_crm
                set delivery_time =
                case when to_char(delivery_time,'hh24') = '07' then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' ' || to_char(((timestamp '1970-01-01 00:00:00' + numtodsinterval((select dbms_random.value(start_seed,end_seed) from camp_sms_type_timegroup where sms_type = 'SMS OFFER CAMPAIGN' and time_group = '01'), 'SECOND'))), 'HH24:mi:ss'), 'mm/dd/yyyy hh24:mi:ss')
                     when to_char(delivery_time,'hh24') = '08' then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' ' || to_char(((timestamp '1970-01-01 00:00:00' + numtodsinterval((select dbms_random.value(start_seed,end_seed) from camp_sms_type_timegroup where sms_type = 'SMS OFFER CAMPAIGN' and time_group = '01'), 'SECOND'))), 'HH24:mi:ss'), 'mm/dd/yyyy hh24:mi:ss')
                     when to_char(delivery_time,'hh24') = '09' then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' ' || to_char(((timestamp '1970-01-01 00:00:00' + numtodsinterval((select dbms_random.value(start_seed,end_seed) from camp_sms_type_timegroup where sms_type = 'SMS OFFER CAMPAIGN' and time_group = '02'), 'SECOND'))), 'HH24:mi:ss'), 'mm/dd/yyyy hh24:mi:ss')
                     when to_char(delivery_time,'hh24') = '10' then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' ' || to_char(((timestamp '1970-01-01 00:00:00' + numtodsinterval((select dbms_random.value(start_seed,end_seed) from camp_sms_type_timegroup where sms_type = 'SMS OFFER CAMPAIGN' and time_group = '03'), 'SECOND'))), 'HH24:mi:ss'), 'mm/dd/yyyy hh24:mi:ss')
                     when to_char(delivery_time,'hh24') = '11' then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' ' || to_char(((timestamp '1970-01-01 00:00:00' + numtodsinterval((select dbms_random.value(start_seed,end_seed) from camp_sms_type_timegroup where sms_type = 'SMS OFFER CAMPAIGN' and time_group = '04'), 'SECOND'))), 'HH24:mi:ss'), 'mm/dd/yyyy hh24:mi:ss')
                     when to_char(delivery_time,'hh24') = '12' then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' ' || to_char(((timestamp '1970-01-01 00:00:00' + numtodsinterval((select dbms_random.value(start_seed,end_seed) from camp_sms_type_timegroup where sms_type = 'SMS OFFER CAMPAIGN' and time_group = '05'), 'SECOND'))), 'HH24:mi:ss'), 'mm/dd/yyyy hh24:mi:ss')
                     when to_char(delivery_time,'hh24') = '13' then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' ' || to_char(((timestamp '1970-01-01 00:00:00' + numtodsinterval((select dbms_random.value(start_seed,end_seed) from camp_sms_type_timegroup where sms_type = 'SMS OFFER CAMPAIGN' and time_group = '06'), 'SECOND'))), 'HH24:mi:ss'), 'mm/dd/yyyy hh24:mi:ss')
                     when to_char(delivery_time,'hh24') = '14' then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' ' || to_char(((timestamp '1970-01-01 00:00:00' + numtodsinterval((select dbms_random.value(start_seed,end_seed) from camp_sms_type_timegroup where sms_type = 'SMS OFFER CAMPAIGN' and time_group = '07'), 'SECOND'))), 'HH24:mi:ss'), 'mm/dd/yyyy hh24:mi:ss')
                     when to_char(delivery_time,'hh24') = '15' then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' ' || to_char(((timestamp '1970-01-01 00:00:00' + numtodsinterval((select dbms_random.value(start_seed,end_seed) from camp_sms_type_timegroup where sms_type = 'SMS OFFER CAMPAIGN' and time_group = '08'), 'SECOND'))), 'HH24:mi:ss'), 'mm/dd/yyyy hh24:mi:ss')
                     when to_char(delivery_time,'hh24') = '16' then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' ' || to_char(((timestamp '1970-01-01 00:00:00' + numtodsinterval((select dbms_random.value(start_seed,end_seed) from camp_sms_type_timegroup where sms_type = 'SMS OFFER CAMPAIGN' and time_group = '09'), 'SECOND'))), 'HH24:mi:ss'), 'mm/dd/yyyy hh24:mi:ss')
                     when to_char(delivery_time,'hh24') = '17' then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' ' || to_char(((timestamp '1970-01-01 00:00:00' + numtodsinterval((select dbms_random.value(start_seed,end_seed) from camp_sms_type_timegroup where sms_type = 'SMS OFFER CAMPAIGN' and time_group = '10'), 'SECOND'))), 'HH24:mi:ss'), 'mm/dd/yyyy hh24:mi:ss')
                     else to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' ' || to_char(((timestamp '1970-01-01 00:00:00' + numtodsinterval((select dbms_random.value(start_seed,end_seed) from camp_sms_type_timegroup where sms_type = 'SMS OFFER CAMPAIGN' and time_group = '10'), 'SECOND'))), 'HH24:mi:ss'), 'mm/dd/yyyy hh24:mi:ss') end
         where rowid = row_id;
        end loop;
        close cur_row;
        AP_PUBLIC.CORE_LOG_PKG.pEnd;
        commit;
        pStats(UPPER('camp_sms_ad_hoc_crm'));
        
        AP_PUBLIC.CORE_LOG_PKG.pStart('Insert to app_bicc.sms_ad_hoc_master_crm');
        insert into app_bicc.sms_ad_hoc_master_crm (id_cuid, text_contract_number, text_phone_number, message, message_code, message_owner, send_to_dwh_reader,
               dtime_message_inserted, dtime_message_sent, dtime_message_expired)
        with control as
        (
            select id_cuid || '|' || message_code || '|' || text_phone_number id_cuid from app_bicc.sms_ad_hoc_master_crm
            where dtime_message_sent >= trunc(dt_current) and message_code in (select message_code from camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type IN ('OFFER REGULAR', 'APP', 'INBOUND', 'LANDING'))
        )
        select cuid id_cuid, contract_code text_contract_number, phone text_phone_number, message, code_promo message_code, 'CRM' message_owner,
               'N' send_to_dwh_reader, dt_current dtime_message_inserted, delivery_time dtime_message_sent, trunc(delivery_time) +1 dtime_message_expired
        from camp_sms_ad_hoc_crm
        where delivery_time >= trunc(dt_current)
          and code_promo in (select message_code from camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'INBOUND')
          and (cuid || '|' || code_promo || '|' || phone) not in (select nvl(id_cuid,'-99999999') from control);
        AP_PUBLIC.CORE_LOG_PKG.pEnd;
        commit;

END IF;
    
<<FINISH_LINE>>
AP_PUBLIC.CORE_LOG_PKG.PFINISH ;
END;
/

