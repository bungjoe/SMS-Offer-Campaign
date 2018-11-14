create or replace procedure CAMP_SMS_REGULAR as

    delivery varchar(30); dt_current timestamp;     dt_start date;                  dt_end date; 
    horus number;         sms_count number;         day_count number;               csize number;
    hsize number;         contact_num varchar2(50); contact_name varchar2(150);     day_types varchar2(50);
    genders varchar2(50); text_sms varchar(180);    time_group varchar(5);          row_id varchar(100);        message_code varchar2(50);
    cursor cur_mon is
          SELECT csm.contact_number, csm.contact_name, csm.gender from camp_sms_monitor csm
          inner join camp_sms_monitor_detail csd on csm.contact_number = csd.contact_number and csd.status = 1
          where csm.flag_active = 1 and csd.channel = 'SMS OFFER CAMPAIGN';

    cursor cur_row is
           select rowid from camp_sms_ad_hoc_crm where delivery_time >= trunc(dt_current)
           and code_promo in
               (
                    select message_code from camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type ='OFFER REGULAR'
                    union all
                    select 'FF_OFFER_RBP' from dual
               )
           and cuid <> 615898;

PROCEDURE pStats( acTable VARCHAR2, anPerc NUMBER DEFAULT 0.01) IS
BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pStart( 'Stat:'||upper(acTable) );
    DBMS_STATS.Gather_Table_Stats( OwnName => 'AP_CRM', TabName => acTable,Estimate_Percent => anPerc );
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
END;
PROCEDURE pTruncate( acTable VARCHAR2) IS
BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pStart( 'Trunc:'||upper(acTable) );
    EXECUTE IMMEDIATE 'TRUNCATE TABLE AP_CRM.'||upper(acTable) ;
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
END;
begin
    AP_PUBLIC.CORE_LOG_PKG.pInit( 'AP_CRM', 'CAMP_SMS_REGULAR');
    --    dt_current := to_timestamp('01/18/2018 07:30:30','mm/dd/yyyy hh24:mi:ss');
    dt_current := sysdate;
    day_types := get_day_type_calendar(trunc(dt_current));
    
    if day_types <> 'Workday' then
       goto finish_line;
    end if;

    /* Get day count to spread the sms sending */
    with all_day as
    (
        select /*+ MATERIALZIE */ to_char(dt_current,'yymm')campaign_id, trunc(dt_current,'MM') + (level-1) as date_d
        from dual
        connect by (level-1) <= 31
    ),
    t1 as
    (
        select /*+ MATERIALZIE */ to_char(dt_current,'yymm')campaign_id, trunc(dt_current,'MM') + ((level-1)*5) as date_d
        from dual
        connect by ((level-1)*5) <= 31
    ),
    t2 as
    (
        select to_char(dt_current,'yymm')campaign_id, date_d date_start,
        case when rownum < 6 then date_d + 4 else date_d + 5 end date_end,
        rownum  sms_cycle
        from t1 where rownum <= 6
    )
    select t2.date_start, t2.date_end, t2.sms_cycle, count(date_d)
          into dt_start, dt_end, sms_count, day_count
    from all_day
    inner join t2 on all_Day.campaign_id = t2.campaign_id and all_Day.date_d between t2.date_start and t2.date_end
    where all_Day.date_d between trunc(dt_current) and (select date_end from t2 where trunc(dt_current) between date_start and date_end)
    and ALL_DAY.date_d NOT in (SELECT trunc(DATE_SKIP) FROM CAMP_CFG_SMS_SKIP WHERE SMS_TYPE = 'SMS OFFER CAMPAIGN' and last_Day(trunc(date_skip)) = last_day(trunc(dt_current)))
    group by t2.date_start, t2.date_end, t2.sms_cycle;

    select count(cal.date_id) into day_count from camp_calendar cal 
    where (cal.date_id between trunc(sysdate) and dt_end)
      and cal.isholiday = 0 and cal.isweekday = 1;
    /********************************************************************************************************************************************************************************/
    dbms_output.put_line(dt_start);
    dbms_output.put_line(dt_end);
    --goto finish_line;
    /* Flush sms record with delivery time of today if any */
    AP_PUBLIC.CORE_LOG_PKG.pStart('Flush sms record with delivery time of today if any');
    delete from ap_crm.camp_sms_ad_hoc_crm       
    where delivery_time >= trunc(dt_current)
        and code_promo in
           (
                select message_code from camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type ='OFFER REGULAR'
                union all
                select 'FF_OFFER_RBP' from dual
           );
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    --pStats('camp_sms_ad_hoc_crm');

    /* Generate list of sms to send */
    ptruncate('gtt_camp_sms_base');
    AP_PUBLIC.CORE_LOG_PKG.pStart('Building sms base list');
    insert /*+ APPEND */ into gtt_camp_sms_base
    with exc as
    (
        select /*+ MATERIALIZE */ to_number(cuid)cuid from ap_crm.camp_sms_ad_hoc_crm where (delivery_time >= dt_start and delivery_time <= (dt_end+1) - (1/24/60/60))
        and code_promo in
            (
                (select message_code from camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR')
                union all
                select 'FF_OFFER_RBP' from dual
            )
        union all
        SELECT to_number(ID_CUID) FROM AP_CRM.CAMP_SMS_FF_BASE
        WHERE CAMPAIGN_ID = TO_CHAR(dt_current, 'YYMM') AND CALL_TO_ACTION IN ('APP', 'LANDING', 'INBOUND')
        union all
        /* find From Mobile App Form */
        select nvl(to_number(CUID),-99999) from AP_CRM.V_CAMP_MOBILE_APP      
    )
    select /*+ USE_HASH(CCL AX)  */ distinct ccl.skp_client, ax.cuid, ax.contract_id, ax.mobile1, pilot_flag, to_number(ccl.TDY_PRIORITY)TDY_PRIORITY
           ,'REG' flag_sms
    from camp_tdy_call_list ax
    inner join camp_compiled_list ccl on ax.cuid = ccl.id_cuid
    where 1=1
    and (lower(ax.info2) not like '6.fol%' or lower(ax.info2) not like '%disaster%' or lower(ax.info2) not like '%landing_page 1-30%')
    and coalesce(ccl.dt_complaint, ccl.dt_dwto, ccl.dt_dwto2) is null
    and ax.cuid not in (select nvl(cuid,-999999) from exc)
    ;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('gtt_camp_sms_base');
    
    select count(1) into csize from ap_crm.gtt_camp_sms_base;
    if csize = 0 then
      goto finish_line;
    end if;
    csize := 0;
    
    AP_PUBLIC.CORE_LOG_PKG.pStart('DEL:CAMP_SMS_REG_TEXT');
    delete from camp_sms_reg_text where campaign_id = to_char(dt_current,'yymm') and phone_update is null;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('CAMP_SMS_REG_TEXT');
    
    /* Update sms text contents */
    pTruncate(upper('gtt_camp_sms_reg_text'));
    AP_PUBLIC.CORE_LOG_PKG.pStart('Insert into gtt_camp_sms_reg_text');
    insert into gtt_camp_sms_reg_text
    with A as
    (
      select distinct /*+ MATERIALIZE */ to_char(eb.valid_from, 'yymm') CAMPAIGN_ID,
        skp_Client,                               ID_CUID,
        ca_limit_final_updated MAX_CREDIT_AMOUNT, ANNUITY_LIMIT_FINAL_UPDATED MAX_ANNUITY,
        priority_actual PRIORITY,                 VALID_FROM,
        VALID_TO,                                 RISK_BAND RISK_GROUP,
        RBP_SEGMENT_TEMP,                         floor(((interest_rate/100) * ca_limit_final_updated * max_tenor + ca_limit_final_updated)/max_tenor) + 5000 as instalment,
        max_tenor tenor
      from ap_crm.camp_elig_base eb
      WHERE 1=1
        and eb.skp_client in (select skp_client from gtt_camp_sms_base)
        /*and (eb.skp_client, valid_from) in 
            (
                 select skp_client, max(valid_from)valid_from from camp_elig_base 
                 where eligible_final_flag = 1 and priority_actual > 0 
                 group by skp_client
            )
        and eb.id_Cuid not in 
            (
                 select nvl(id_cuid,-999999999) from camp_elig_daily_check 
                 where trunc(sysdate) between date_valid_from and date_valid_to and nvl(flag_still_eligible,'U') = 'N'
            )*/
    ),
    upload_hosel as
    (
      select /*+ MATERIALIZE */ fcc.skp_client, fcc.skf_campaign_client, fcc.skp_Campaign, fcc.name_offer, fcc.DATE_VALID_FROM, fcc.DATE_VALID_TO
      from ap_crm.camp_client_at fcc
      where LOWER(flag_deleted) = 'n' and nvl(fcc.skp_Client,-99999) in (select nvl(skp_Client,'-99999') from A)
    ),
    base as
    (
        select /*+ MATERIALIZE */ distinct A.*,
        case  when (cci.name_first like 'Drs' or cci.name_first like 'Drs.') and cci.name_middle='XNA' then cci.name_last
              when (cci.name_first like 'Drs' or cci.name_first like 'Drs.') and cci.name_middle<>'XNA' then cci.name_middle
              when (cci.name_first like 'Ir' or cci.name_first like 'Ir.') and cci.name_middle='XNA' then cci.name_last
              when (cci.name_first like 'Ir' or cci.name_first like 'Ir.') and cci.name_middle<>'XNA' then cci.name_middle
              when (cci.name_first like 'H' or cci.name_first like 'H.') and cci.name_middle='XNA' then cci.name_last
              when (cci.name_first like 'H' or cci.name_first like 'H.') and cci.name_middle<>'XNA' then cci.name_middle
              when (cci.name_first like 'Hj' or cci.name_first like 'Hj.') and cci.name_middle='XNA' then cci.name_last
              when (cci.name_first like 'Hj' or cci.name_first like 'Hj.') and cci.name_middle<>'XNA' then cci.name_middle
              when cci.name_first in ('A', 'B', 'C', 'D', 'E', 'F',  'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
                    'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',  'X',  'Y', 'Z') and cci.name_middle='XNA' then cci.name_last
              when cci.name_first in ('A',  'B', 'C', 'D',  'E', 'F',  'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
                    'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',  'W', 'X',  'Y', 'Z') and cci.name_middle<>'XNA' then cci.name_middle
              when cci.name_first in ('A.', 'B.', 'C.', 'D.',  'E.',  'F.',  'G.', 'H.', 'I.', 'J.', 'K.', 'L.', 'M.', 'N.',
                    'O.', 'P.', 'Q.', 'R.', 'S.', 'T.', 'U.',  'V.',  'W.',  'X.', 'Y.', 'Z.') and cci.name_middle='XNA' then cci.name_last
              when cci.name_first in ('A.', 'B.', 'C.', 'D.', 'E.',  'F.', 'G.', 'H.', 'I.', 'J.', 'K.', 'L.', 'M.', 'N.',
                    'O.', 'P.', 'Q.', 'R.', 'S.', 'T.', 'U.',  'V.', 'W.',  'X.', 'Y.', 'Z.') and cci.name_middle<>'XNA' then cci.name_middle
        else cci.name_first end as name_update,
        case  when gender = 'Male' then 'Bpk'
              when gender = 'Female' then 'Ibu'
        else ' ' end as GENDER_GROUP,
        coalesce(cfp.PHONE1,cfp.PHONE2)phone_update,
        a.instalment,
        a.tenor,
        trunc(round((a.MAX_CREDIT_AMOUNT/1000000),2),1) as cr_amt,
        case  when a.INSTALMENT >=1000000 then round((a.INSTALMENT/1000000),2)||'jt/bln'
              when a.INSTALMENT <1000000 then floor(a.INSTALMENT/1000)||'rb/bln'
        end as ins,
        'Dynamic' random_split,
        'other' final_flag
        from A
        inner join AP_CRM.CAMP_CLIENT_IDENTITY cci on a.skp_client = cci.skp_client
        inner join AP_CRM.V_CAMP_FINAL_PHONENUM cfp on a.id_cuid = cfp.id_cuid
        where nvl(A.skp_client,-9999) in (select nvl(skp_Client,'-9999') from upload_hosel)
        and (cfp.PHONE1 is not null or cfp.PHONE2 is not null)
    )
    SELECT  distinct base.campaign_id, base.ID_CUID, base.PRIORITY, base.PHONE_UPDATE, base.cr_amt,  base.ins, base.random_split, base.final_flag,
    case  when random_split='Limit_2' and final_flag in ('other','cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then 'Limit_2'
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_vip' then 'VIP_Pilot'
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_p22+' then 'P22_Pilot'
          when random_split='Dynamic' and final_flag in ('other', 'cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then 'Dynamic'
    end as RANDOM_SPLIT_NEW,
    case  when random_split='Limit_2' and final_flag in ('other','cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Limit_2'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_vip' then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_VIP_Pilot'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_p22+' then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_pilot_p22'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split='Dynamic' and final_flag in ('other', 'cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Dynamic_1'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
    end as SMS_1_5,
    case  when random_split='Limit_2' and final_flag in ('other','cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Limit_2'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_vip' then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_VIP_Pilot'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_p22+' then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_pilot_p22'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split='Dynamic' and final_flag in ('other', 'cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Dynamic_2'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
    end as SMS_6_10,
    case  when random_split='Limit_2' and final_flag in ('other','cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Limit_2'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_vip' then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_VIP_Pilot'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_p22+' then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_pilot_p22'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split='Dynamic' and final_flag in ('other', 'cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Dynamic_3'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
    end as SMS_11_15,
    case  when random_split='Limit_2' and final_flag in ('other','cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Limit_2'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_vip' then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_VIP_Pilot'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_p22+' then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_pilot_p22'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split='Dynamic' and final_flag in ('other', 'cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Dynamic_4'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
    end as SMS_16_20,
    case  when random_split='Limit_2' and final_flag in ('other','cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Limit_2'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_vip' then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_VIP_Pilot'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_p22+' then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_pilot_p22'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split='Dynamic' and final_flag in ('other', 'cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Dynamic_5'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
    end as SMS_21_25,
    case  when random_split='Limit_2' and final_flag in ('other','cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Limit_2'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_vip' then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_VIP_Pilot'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_p22+' then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_pilot_p22'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
          when random_split='Dynamic' and final_flag in ('other', 'cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          replace(replace(replace((select text_content from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Dynamic_6'), '[nick_name]',case when length(name_update) > 10 then 'Nasabah Yth' else  gender_group || ' ' || name_update end),'[instalment]', base.ins), '[credit_amount]', base.cr_amt)
    end as SMS_26_30,
    case  when random_split='Limit_2' and final_flag in ('other','cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Limit_2')
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_vip' then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_VIP_Pilot')
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_p22+' then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_pilot_p22')
          when random_split='Dynamic' and final_flag in ('other', 'cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Dynamic_1')
    end as MSC_1_5,
    case  when random_split='Limit_2' and final_flag in ('other','cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Limit_2')
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_vip' then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_VIP_Pilot')
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_p22+' then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_pilot_p22')
          when random_split='Dynamic' and final_flag in ('other', 'cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Dynamic_2')
    end as MSC_6_10,
    case  when random_split='Limit_2' and final_flag in ('other','cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Limit_2')
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_vip' then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_VIP_Pilot')
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_p22+' then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_pilot_p22')
          when random_split='Dynamic' and final_flag in ('other', 'cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Dynamic_3')
    end as MSC_11_15,
    case  when random_split='Limit_2' and final_flag in ('other','cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Limit_2')
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_vip' then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_VIP_Pilot')
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_p22+' then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_pilot_p22')
          when random_split='Dynamic' and final_flag in ('other', 'cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Dynamic_4')
    end as MSC_16_20,
    case  when random_split='Limit_2' and final_flag in ('other','cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Limit_2')
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_vip' then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_VIP_Pilot')
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_p22+' then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_pilot_p22')
          when random_split='Dynamic' and final_flag in ('other', 'cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Dynamic_5')
    end as MSC_21_25,
    case  when random_split='Limit_2' and final_flag in ('other','cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Limit_2')
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_vip' then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_VIP_Pilot')
          when random_split in ('Limit_2', 'Dynamic') and final_flag='pilot_p22+' then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_pilot_p22')
          when random_split='Dynamic' and final_flag in ('other', 'cg', 'pilot_fake_p1', 'cg_vip', 'cg_fake_p1', 'n/a') then
          (select message_code from ap_Crm.camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' and text_type = 'SMS_Dynamic_6')
    end as MSC_26_30
    from base;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('CAMP_SMS_REG_TEXT');
    
    AP_PUBLIC.CORE_LOG_PKG.pStart('Merge into camp_sms_reg_text');
    MERGE /*+  */ INTO AP_CRM.CAMP_SMS_REG_TEXT TGT
    using
    (
        select distinct * from gtt_camp_sms_reg_text
    )src
        on (src.campaign_id = tgt.campaign_id and tgt.id_cuid = src.id_cuid and tgt.phone_update = src.phone_update)
        when matched then update set
            tgt.random_split = src.random_split,
            tgt.priority     = src.priority,
            tgt.final_flag   = src.final_flag,
            tgt.random_split_new  = src.random_split_new,
            tgt.sms_1_5           = src.sms_1_5,
            tgt.sms_6_10          = src.sms_6_10,
            tgt.sms_11_15         = src.sms_11_15,
            tgt.sms_16_20         = src.sms_16_20,
            tgt.sms_21_25         = src.sms_21_25,
            tgt.sms_26_30         = src.sms_26_30,
            tgt.msc_1_5           = src.msc_1_5,
            tgt.msc_6_10          = src.msc_6_10,
            tgt.msc_11_15         = src.msc_11_15,
            tgt.msc_16_20         = src.msc_16_20,
            tgt.msc_21_25         = src.msc_21_25,
            tgt.msc_26_30         = src.msc_26_30
        when not matched then insert (
            tgt.campaign_id, tgt.id_cuid, tgt.priority, tgt.phone_update, tgt.random_split, tgt.final_flag, tgt.random_split_new, tgt.sms_1_5, tgt.sms_6_10,
            tgt.sms_11_15, tgt.sms_16_20, tgt.sms_21_25, tgt.sms_26_30, tgt.msc_1_5, tgt.msc_6_10, tgt.msc_11_15, tgt.msc_16_20, tgt.msc_21_25, tgt.msc_26_30
        )
        values (
            src.campaign_id, src.id_cuid, src.priority, src.phone_update, src.random_split, src.final_flag, src.random_split_new, src.sms_1_5, src.sms_6_10,
            src.sms_11_15, src.sms_16_20, src.sms_21_25, src.sms_26_30, src.msc_1_5, src.msc_6_10, src.msc_11_15, src.msc_16_20, src.msc_21_25, src.msc_26_30
        );
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('CAMP_SMS_REG_TEXT');
    /*******************************************************************************************************************************************************/
    
    /* Building list of smses to sent */
    ptruncate('gtt_camp_sms_counts');
    AP_PUBLIC.CORE_LOG_PKG.pStart('Collecting counts of smses');
    insert /*+ APPEND */ into gtt_camp_sms_counts
    select cuid, count(cuid)count_sms from camp_sms_ad_hoc_crm
    where delivery_time >= trunc(dt_current,'MM')
    and code_promo in
        (
            (select message_code from camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type ='OFFER REGULAR')
            union all
            select 'FF_OFFER_RBP' from dual
        )
    and cuid in (select cuid from gtt_camp_sms_base)
    group by cuid;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('gtt_camp_sms_counts');

    ptruncate('gtt_camp_sms_last');
    AP_PUBLIC.CORE_LOG_PKG.pStart('Collecting last time of smses sent');
    insert /*+ APPEND  */ into gtt_camp_sms_last
    select cuid, max(delivery_time)send_date from camp_sms_ad_hoc_crm
    where delivery_time = trunc(dt_current,'MM')
    and code_promo in
        (
            (select message_code from camp_cfg_sms_text where campaign_id = to_char(dt_current,'yymm') and sms_type ='OFFER REGULAR')
            union all
            select 'FF_OFFER_RBP' from dual
        )
    and cuid in (select cuid from gtt_camp_sms_base)
    group by cuid;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('gtt_camp_sms_last');

    ptruncate('gtt_camp_sms_list');
    AP_PUBLIC.CORE_LOG_PKG.pStart('Building list of smses to sent');
    insert /*+ APPEND */ into gtt_camp_sms_list
    select /*+ USE_HASH(base sc sms sl) */
    base.skp_client, base.cuid, base.contract_id, base.mobile1, base.pilot_flag, base.TDY_PRIORITY, nvl(sc.count_sms,0)count_sms, trunc(dt_current) - nvl(trunc(sl.send_date),trunc(dt_current)-6) last_sms,
/*        case when nvl(count_sms,0) = 0 then sms.sms_1_5
             when nvl(count_sms,0) = 1 then sms.sms_6_10
             when nvl(count_sms,0) = 2 then sms.sms_11_15
             when nvl(count_sms,0) = 3 then sms.sms_16_20
             when nvl(count_sms,0) = 4 then sms.sms_21_25
             when nvl(count_sms,0) = 5 then sms.sms_26_30
        end sms_text,*/
    case when sms_count = 1 then sms.sms_1_5
         when sms_count = 2 then sms.sms_6_10
         when sms_count = 3 then sms.sms_11_15
         when sms_count = 4 then sms.sms_16_20
         when sms_count = 5 then sms.sms_21_25
         when sms_count = 6 then sms.sms_26_30
    end sms_text,
    CASE WHEN flag_sms = 'RPOS' then 1 else 2 end golem,
/*        case when nvl(count_sms,0) = 0 then sms.msc_1_5
             when nvl(count_sms,0) = 1 then sms.msc_6_10
             when nvl(count_sms,0) = 2 then sms.msc_11_15
             when nvl(count_sms,0) = 3 then sms.msc_16_20
             when nvl(count_sms,0) = 4 then sms.msc_21_25
             when nvl(count_sms,0) = 5 then sms.msc_26_30
        end message_code,        */
    case when sms_count = 1 then sms.msc_1_5
         when sms_count = 2 then sms.msc_6_10
         when sms_count = 3 then sms.msc_11_15
         when sms_count = 4 then sms.msc_16_20
         when sms_count = 5 then sms.msc_21_25
         when sms_count = 6 then sms.msc_26_30
    end message_code
    from gtt_camp_sms_base base
    left join gtt_camp_sms_counts sc on base.cuid = sc.cuid
    left join camp_sms_reg_text sms on base.cuid = sms.id_cuid and campaign_id = to_char(dt_current,'yymm')
    left join gtt_camp_sms_last sl on base.cuid = sl.cuid
    where nvl(sc.count_sms,0) < sms_count;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('gtt_camp_sms_list');

    ptruncate('gtt_camp_sms_offer');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS:GTT_CAMP_SMS_OFFER');
    insert /*+ APPEND */ into gtt_camp_sms_offer
    select distinct * from gtt_camp_sms_list ls where sms_text is not null and last_sms >= 1;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('GTT_CAMP_SMS_OFFER');
    /*******************************************************************************************************************************************************/

    /* Distributing sms delivery to every hours */
    select count(1) into csize from ap_crm.gtt_camp_sms_offer;
    select distinct text_content, message_code into text_sms, message_code from ap_Crm.camp_cfg_sms_text 
    where campaign_id = to_char(dt_current,'yymm') and sms_type = 'OFFER REGULAR' AND TO_NUMBER(SUBSTR(TEXT_TYPE, -1, 1)) = SMS_COUNT; --and text_type = 'SMS_Dynamic_1';
    
    if csize > 0 then

        select ceil(count(skp_client)/day_count) into csize from ap_crm.gtt_camp_sms_offer;
        hsize := ceil(csize/10);
        AP_PUBLIC.CORE_LOG_PKG.pStart('INS:DISTRIBUTE REG SMS');
        insert into ap_crm.camp_sms_ad_hoc_crm
        with base as (
            select row_number() over (order by nums asc, count_sms asc, last_sms desc, tdy_priority asc)numsa, ofr.* from ap_crm.gtt_camp_sms_offer ofr
        )
        select distinct mobile1 phone, 'GINQ' comm_type, 'GI_OTHERS' comm_subtype,
        case when numsa <= hsize * 1 then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' 08:00:00','mm/dd/yyyy hh24:mi:ss')
             when numsa <= hsize * 2 then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' 09:00:00','mm/dd/yyyy hh24:mi:ss')
             when numsa <= hsize * 3 then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' 10:00:00','mm/dd/yyyy hh24:mi:ss')
             when numsa <= hsize * 4 then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' 11:00:00','mm/dd/yyyy hh24:mi:ss')
             when numsa <= hsize * 5 then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' 12:00:00','mm/dd/yyyy hh24:mi:ss')
             when numsa <= hsize * 6 then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' 13:00:00','mm/dd/yyyy hh24:mi:ss')
             when numsa <= hsize * 7 then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' 14:00:00','mm/dd/yyyy hh24:mi:ss')
             when numsa <= hsize * 8 then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' 15:00:00','mm/dd/yyyy hh24:mi:ss')
             when numsa <= hsize * 9 then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' 16:00:00','mm/dd/yyyy hh24:mi:ss')
             when numsa <= hsize * 10 then to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' 17:00:00','mm/dd/yyyy hh24:mi:ss')
        else to_timestamp(to_char(dt_current,'mm/dd/yyyy') || ' 17:59:00','mm/dd/yyyy hh24:mi:ss')
        end delivery_time,
        cuid id_cuid, contract_id contract_code, sms_text message, 'N' flag_sent_to_provdr, sysdate dtime_inserted, null dtime_sent_to_provdr, 'CRM' Department, 'DL' comm_status, 'CLS' comm_result,
        base.message_code code_promo, null
        from base where numsa <= csize;

        if not (cur_mon%ISOPEN) then
            open cur_mon;
        end if;
        loop
        fetch cur_mon into contact_num, contact_name, genders;
        exit when cur_mon%NOTFOUND;
          for horus in 8..17
          loop
            delivery := to_char(dt_current,'mm/dd/yyyy') || ' ' || to_char(horus, '09') || ':00:00';
            insert into ap_CRM.CAMP_sms_ad_hoc_crm values (contact_num, 'GINQ', 'GI_OTHERS', to_timestamp(delivery,'mm/dd/yyyy hh24:mi:ss'), '615898', '3608717024',
            replace(replace(replace(text_sms,'[credit_amount]','25'),'[instalment]','625rb/bln'),'[nick_name]', 
            case when length(trim(contact_name)) > 12 then 'Nasabah Yth' else 'Yth. ' || case when genders = 'Male' then 'Bpk' when genders = 'Female' then 'Ibu' end || ' ' ||trim(initcap(contact_name)) end),
              'N', sysdate, null,'CRM','DL','CLS',message_code,'SMS_Dynamic_1');
          end loop;
        horus := 8;
        end loop;
        close cur_mon;
        AP_PUBLIC.CORE_LOG_PKG.pEnd;
        commit;
        --pStats(UPPER('camp_sms_ad_hoc_crm'));

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
        /***************************************************************************************************************************************************/

        AP_PUBLIC.CORE_LOG_PKG.pStart('Insert to app_bicc.sms_ad_hoc_master_crm');
        INSERT INTO app_bicc.SMS_AD_HOC_MASTER_CRM
        (
            ID_CUID,            TEXT_CONTRACT_NUMBER,
            TEXT_PHONE_NUMBER,  MESSAGE,
            MESSAGE_CODE,       MESSAGE_OWNER,
            SEND_TO_DWH_READER, DTIME_MESSAGE_INSERTED,
            DTIME_MESSAGE_SENT, DTIME_MESSAGE_EXPIRED
        )
        WITH
        W$1 AS 
        ( 
            SELECT /*+ FULL(A) */ DISTINCT MESSAGE_CODE
            FROM AP_CRM.CAMP_CFG_SMS_TEXT A
            WHERE CAMPAIGN_ID = TO_CHAR(SysDate, 'yymm')
            AND SMS_TYPE IN ('OFFER REGULAR', 'APP', 'INBOUND', 'LANDING')
        ),
        W$2 AS 
        (
            SELECT /*+ FULL(A) */ DISTINCT MESSAGE_CODE
            FROM AP_CRM.CAMP_CFG_SMS_TEXT A
            WHERE CAMPAIGN_ID = TO_CHAR(SysDate, 'yymm')
            AND SMS_TYPE = 'OFFER REGULAR'
        ),
        CONTROL AS
        ( 
            SELECT /*+ MATERIALIZE */ DISTINCT NVL(A.ID_CUID || '|' || A.MESSAGE_CODE || '|' || A.TEXT_PHONE_NUMBER, '-99999999') ID_CUID
            FROM APP_BICC.SMS_AD_HOC_MASTER_CRM A
            JOIN W$1 B ON B.MESSAGE_CODE = A.MESSAGE_CODE
            WHERE A.DTIME_MESSAGE_SENT >= TRUNC(SysDate)
        )
        SELECT A.CUID ID_CUID,
               A.CONTRACT_CODE TEXT_CONTRACT_NUMBER,
               A.PHONE TEXT_PHONE_NUMBER,
               A.MESSAGE,
               A.CODE_PROMO MESSAGE_CODE,
               'CRM' MESSAGE_OWNER,
               'N' SEND_TO_DWH_READER,
               SysDate DTIME_MESSAGE_INSERTED,
               A.DELIVERY_TIME DTIME_MESSAGE_SENT,
               TRUNC(A.DELIVERY_TIME) + 1 DTIME_MESSAGE_EXPIRED
        FROM AP_CRM.CAMP_SMS_AD_HOC_CRM A
        JOIN W$2 B ON B.MESSAGE_CODE = A.CODE_PROMO
        LEFT JOIN CONTROL C ON C.ID_CUID = (A.CUID || '|' || A.CODE_PROMO || '|' || A.PHONE)
        WHERE A.DELIVERY_TIME >= TRUNC(SysDate)
        AND C.ID_CUID IS NULL;
        AP_PUBLIC.CORE_LOG_PKG.pEnd;
        commit;

    end if;
<<finish_line>>
AP_PUBLIC.CORE_LOG_PKG.pFinish;
end;
