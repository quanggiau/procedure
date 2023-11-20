-- Function: dm.dm_mst_shohin_create(character varying, character varying, character varying)

-- DROP FUNCTION dm.dm_mst_shohin_create(character varying, character varying, character varying);

CREATE OR REPLACE FUNCTION dm.dm_mst_shohin_create(record_date character varying, job_id character varying, net_id character varying)
  RETURNS integer AS
$BODY$
DECLARE
    --　set cursor
    ret_num int; -- 戻り値 0:正常終了/1:異常終了/2:警告終了
    exp_str varchar(1000);
    user_id character varying(200);
    sys_datetime timestamp;
    product_state int;
    deleteflag_state int;
    insert_count bigint;
    update_count bigint;
    delete_count bigint;
    ret_log int;
    err_msg varchar(1000);
BEGIN
    -- STEP 1
    SELECT * INTO ret_log FROM DM.output_log('0','商品マスタ作成処理を開始しました。', job_id, net_id, 'DM_MST_SHOHIN_CREATE', NULL, NULL, NULL, NULL, NULL, NULL);
    SELECT char_data1 INTO user_id FROM DM.MST_CONTROL WHERE record_kbn = '0120' AND data_cd = '00';
    SELECT CURRENT_TIMESTAMP INTO sys_datetime;

    -- STEP 2
    SELECT * INTO update_count, insert_count, product_state FROM dm.dm_mst_shohin_create_product_master(job_id, net_id, user_id, sys_datetime);
    IF product_state = 0 THEN
        -- STEP 3
        SELECT * INTO delete_count, deleteflag_state FROM dm.dm_mst_shohin_create_delete_flag(job_id, net_id, user_id, sys_datetime);
        IF deleteflag_state = 0 THEN
            -- STEP 4
            SELECT * INTO err_msg FROM FORMAT ('商品マスタ作成処理が完了しました。（INSERT：%s件、UPDATE：%s件、DELETE:%s件）', insert_count, update_count, delete_count) ;
            SELECT * INTO ret_log FROM DM.output_log('0',err_msg, job_id, net_id, 'DM_MST_SHOHIN_CREATE', NULL, NULL, NULL, NULL, NULL, NULL);
            ret_num = 0;
        ELSE
            ret_num = 1;
        END IF;
    ELSE
        ret_num = 1;
    END IF;

    RETURN ret_num;

    EXCEPTION
    WHEN OTHERS THEN
    ret_num = 1;
    select SQLSTATE || ':' || SQLERRM INTO exp_str;
    SELECT * INTO ret_log FROM DM.output_log('0',exp_str, job_id, net_id, 'DM_MST_SHOHIN_CREATE', NULL, NULL, NULL, NULL, NULL, NULL);
    return ret_num;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

-- Function: dm.dm_mst_shohin_create_product_master(character varying, character varying, character varying, timestamp without time zone)

-- DROP FUNCTION dm.dm_mst_shohin_create_product_master(character varying, character varying, character varying, timestamp without time zone);

CREATE OR REPLACE FUNCTION dm.dm_mst_shohin_create_product_master(IN job_id character varying, IN net_id character varying, IN user_id character varying, IN sys_datetime timestamp without time zone, OUT update_count bigint, OUT insert_count bigint, OUT ret_num integer)
  RETURNS record AS
$BODY$
DECLARE
    --set cursor
    exp_str varchar(1000);
    rec RECORD;
    ret_log int;
    wk_shohin_id bigint;
BEGIN
    -- STEP 2
    -- STEP 2-1
    SELECT * INTO ret_log FROM DM.output_log('0','DM.商品マスタ作成処理を開始しました。', job_id, net_id, 'DM_MST_SHOHIN_CREATE', NULL, NULL, NULL, NULL, NULL, NULL);
    update_count = 0;
    insert_count = 0;
    FOR rec IN SELECT * FROM DS.MOAIF1002 ORDER BY DS.MOAIF1002.productgroupid LOOP
        -- STEP 2-2

        IF EXISTS (SELECT 1 FROM DM.MST_SHOHIN WHERE DM.MST_SHOHIN.product_group_id = rec.productgroupid) THEN
            -- STEP 2-4
            SELECT DM.MST_SHOHIN.shohin_id INTO wk_shohin_id FROM DM.MST_SHOHIN WHERE DM.MST_SHOHIN.product_group_id = rec.ProductGroupID;
            UPDATE DM.MST_SHOHIN
            SET product_group_id = rec.ProductGroupID,
                product_group_code = rec.ProductGroupCode,
                product_series_code = rec.ProductSeriesCode,
                product_category_desc = rec.ProductCategoryDesc,
                del_flg = rec.del_flg::int::smallint,
                upd_user_id = user_id,
                upd_datetime = sys_datetime,
                upd_prg = 'DM_MST_SHOHIN_CREATE'
            WHERE DM.MST_SHOHIN.shohin_id = wk_shohin_id;
            update_count = update_count + 1;
        ELSE
            -- STEP 2-3
            SELECT nextval('dm.seq_shohin_id') INTO wk_shohin_id;

            -- STEP 2-4
            INSERT INTO DM.MST_SHOHIN(shohin_id, product_group_id, product_group_code, product_series_code, product_category_desc, del_flg, ins_user_id, ins_datetime, ins_prg, upd_user_id, upd_datetime, upd_prg)
            VALUES (wk_shohin_id, rec.ProductGroupID, rec.ProductGroupCode, rec.ProductSeriesCode, rec.ProductCategoryDesc, rec.del_flg::int::smallint, user_id, sys_datetime, 'DM_MST_SHOHIN_CREATE', user_id, sys_datetime, 'DM_MST_SHOHIN_CREATE');
            insert_count = insert_count + 1;
        END IF;
    END LOOP;
    ret_num = 0;

    EXCEPTION
    WHEN OTHERS THEN
    ret_num = 1;
    select SQLSTATE || ':' || SQLERRM INTO exp_str;
    SELECT  * INTO ret_log FROM DM.output_log('0',exp_str, job_id, net_id, 'DM_MST_SHOHIN_CREATE', NULL, NULL, NULL, NULL, NULL, NULL);
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

-- Function: dm.dm_mst_shohin_create_delete_flag(character varying, character varying, character varying, timestamp without time zone)

-- DROP FUNCTION dm.dm_mst_shohin_create_delete_flag(character varying, character varying, character varying, timestamp without time zone);

CREATE OR REPLACE FUNCTION dm.dm_mst_shohin_create_delete_flag(IN job_id character varying, IN net_id character varying, IN user_id character varying, IN sys_datetime timestamp without time zone, OUT delete_count bigint, OUT ret_num integer)
  RETURNS record AS
$BODY$
DECLARE
    --set cursor
    ret_log int;
    del_msg varchar(1000);
    exp_str varchar(1000);
BEGIN
    -- STEP 3
    -- STEP 3-1
    SELECT * INTO ret_log FROM DM.output_log('0','DM.商品マスタ削除処理を開始しました。', job_id, net_id, 'DM_MST_SHOHIN_CREATE', NULL, NULL, NULL, NULL, NULL, NULL);
    delete_count = 0;
    -- STEP 3-2
    DELETE FROM DM.MST_SHOHIN
    WHERE DM.MST_SHOHIN.upd_datetime <> sys_datetime;
    IF FOUND THEN 
        GET DIAGNOSTICS delete_count = ROW_COUNT;
     End if; 
    ret_num = 0;

    EXCEPTION
    WHEN OTHERS THEN
     ret_num = 1;
    select SQLSTATE || ':' || SQLERRM INTO exp_str;
    SELECT  * INTO ret_log FROM DM.output_log('0',exp_str, job_id, net_id, 'DM_MST_SHOHIN_CREATE', NULL, NULL, NULL, NULL, NULL, NULL);
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
  