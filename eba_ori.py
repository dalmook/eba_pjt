#python "D:\GOC 교육\python코드\240913_ebatogscmdbtosunipgo_HBM순입고변경.py" http://edm2.sec.samsung.net/cc/link/verLink/172888926297004430/20 "★★확정EBA)10월_2개년_10월_W41guide" "10연전_확정"
# python "D:\GOC 교육\python코드\250331_ebatogscmdbtosunipgo.py" http://edm2.sec.samsung.net/cc/link/verLink/174175468696804582/16 "★★EBA_확정) 25.3월 2개년" "3월_2개년"
# python "D:\GOC 교육\python코드\250331_ebatogscmdbtosunipgo.py" http://edm2.sec.samsung.net/cc/link/verLink/174338140443604632/2 "작성중)4월2개년초안" "4월_2개년"


import argparse
import os
import time
from datetime import datetime

import sys
import logging
import pandas as pd
import numpy as np

import urllib3
import urllib3.exceptions

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver import EdgeOptions
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from webdriver_manager.core.download_manager import WDMDownloadManager
from webdriver_manager.core.http import HttpClient
from webdriver_manager.microsoft import EdgeChromiumDriverManager

from selenium import webdriver

#!pip install cx_Oracle
import cx_Oracle as cx

# pip instakk xkwings
import xlwings as xw

from openpyxl import load_workbook
from openpyxl.styles import numbers
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

# 시간 설정
start_time = datetime.now()

def print_execution_time(start_time):
    end_time=datetime.now()
    """Calculates and prints the execution time in days, hours, minutes, and seconds."""
    elapsed_time = end_time - start_time
    days = elapsed_time.days
    hours, remainder = divmod(elapsed_time.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    print(f"Script started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Script ended at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total execution time: {days} days, {hours} hours, {minutes} minutes, and {seconds} seconds")

# Oracle SQL 관련
## 테이블 삭제 함수
def delete_table_if_exists(table_name):
    cursor = conn.cursor()
    try:
        cursor.execute(f"DROP TABLE {table_name}")
        print(f"테이블 {table_name}이 성공적으로 삭제되었습니다.")
    except cx.DatabaseError as e:
        error, = e.args
        if "ORA-00942" in error.message:  # ORA-00942: table or view does not exist
            print(f"테이블 {table_name}이 존재하지 않아 삭제할 필요가 없습니다.")
        else:
            print(f"테이블 삭제 중 오류 발생: {error.message}")
    finally:
        cursor.close()

## 테이블 구조 복사 함수
def copy_table_structure(old_table_name, new_table_name):
    cursor = conn.cursor()
    create_table_query = f"CREATE TABLE {new_table_name} AS SELECT * FROM {old_table_name} WHERE 1=0"
    try:
        cursor.execute(create_table_query)
        print(f"테이블 {new_table_name}가 기존 테이블 {old_table_name}의 구조로 성공적으로 생성되었습니다.")
    except cx.DatabaseError as e:
        error, = e.args
        print(f"테이블 생성 중 오류 발생: {error.message}")
    finally:
        cursor.close()

## 테이블 행 추가 함수
def insert_dataframe_into_table(df, table_name):
    cursor = conn.cursor()
    insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join([':'+str(i+1) for i in range(len(df.columns))])})"
    data = [tuple(row) for row in df.itertuples(index=False, name=None)]
    print('length of data: ', len(data))
    
    for idx, row in enumerate(data):
        try:
            cursor.execute(insert_query, row)
            conn.commit()
        except cx.DatabaseError as e:
            print(idx, row)
            error, = e.args
            print(f"Row {idx}에서 오류 발생: {error.message} - 데이터: {row}")
            # 특정 열 데이터 타입 확인
            print(f"해당 행의 데이터 타입: {[type(r) for r in row]}")

## 기존 테이블/새로운 테이블 명
old_table_name = 'gui_eba_2yr'
new_table_name = 'gui_eba_2yr_test'

## 순입고 관련 쿼리
query=f"""


--2026 2027 2개년 순입고

WITH IPGO_PORTION_2627 AS (
        SELECT FAM6, LINE, YEARMONTH, YEARMONTH_IPGO,TAT_BE,
               RATIO_TO_REPORT(NUM_VALUE) OVER(PARTITION BY FAM6, LINE, YEARMONTH, TAT_BE) AS IPGO_PORTION,
               RATIO_TO_REPORT(NUM_VALUE) OVER(PARTITION BY FAM6, LINE, YEARMONTH, TAT_BE) WF_PORTION,              
               CASE WHEN SUBSTR(YEARMONTH,1,4)!='2025' THEN RATIO_TO_REPORT(NUM_VALUE) OVER(PARTITION BY FAM6,  LINE, YEARMONTH, TAT_BE) ELSE 0 END WF_AVAIL_PORTION,              
               CASE WHEN SUBSTR(YEARMONTH_IPGO,1,4)!='2025' THEN RATIO_TO_REPORT(NUM_VALUE) OVER(PARTITION BY FAM6,  LINE, YEARMONTH, TAT_BE) ELSE 0 END IPGO_AVAIL_PORTION
        FROM
        (
            SELECT FAM6, LINE, YEARMONTH, YEARMONTH_IPGO, COUNT(*) NUM_VALUE, max(TAT_BE) TAT_BE
            FROM
            (
                SELECT FAM6, LINE, YEARMONTH, DATEID,  TAT_BE, TO_CHAR(TAT_BE+DATEID,'yyyymm') YEARMONTH_IPGO
                FROM
                (
                    SELECT  A.FAM6_ADJ FAM6, YEARMONTH, 
                            LINE,
                            CASE 
                                WHEN LINE = '4' THEN 'PFB4'
                                WHEN LINE = '3' THEN 'PFB3'
                                WHEN LINE = 'B' THEN 'PFBB'
                                WHEN LINE = 'P' THEN 'PFBP'
                                WHEN LINE = 'J' THEN 'KFBJ'
                                WHEN LINE = 'H' THEN 'KFBH'
                                WHEN LINE = 'G' THEN 'KFBG'
                                WHEN LINE = 'E' THEN 'KFBE'
                                WHEN LINE = 'C' THEN 'KFBC'
                                WHEN LINE = 'W' THEN 'KFBW'
                                WHEN LINE = 'L' THEN 'XFB2'
                                WHEN LINE = 'M' THEN 'XFB1'                                
                            ELSE '-'
                            END AS LINE_CODE
                          , DECODE(GUBUN,'C_TOT',DECODE(ORDER_NUM,-3, PRE_M9, -2,PRE_M10,-1,PRE_M11,0,PRE_M12, 1,Y0M1, 2,Y0M2, 3, Y0M3, 4,Y0M4, 5,Y0M5, 6,Y0M6, 7,Y0M7, 8,Y0M8, 9,Y0M9, 10,Y0M10, 11,Y0M11, 12,Y0M12, 13, Y1M1, 14,Y1M2, 15, Y1M3, 16,Y1M4, 17,Y1M5, 18,Y1M6, 19,Y1M7, 20,Y1M8, 21,Y1M9, 22,Y1M10, 23,Y1M11, 24,Y1M12,0),0) TAT_BE
                    
                    FROM {new_table_name} A
                    , (
                            SELECT YEARMONTH, ROW_NUMBER() OVER( ORDER BY YEARMONTH)-4 ORDER_NUM
                            FROM
                            (
                                SELECT DISTINCT FNCMONTH YEARMONTH FROM MST_FNC_TIME
                                WHERE FNCMONTH >= '202509'
                                AND FNCMONTH <= '202712'
                            )
                    ) B

                    WHERE 1=1
                    AND PLANID = :plan_id
                    AND GUBUN IN ('C_TOT')
                    
                ) P, MST_FNC_TIME Q
                WHERE P.YEARMONTH=Q.FNCMONTH
                AND FNCMONTH>='202509'
                AND  FNCMONTH<='202712'
            )
            GROUP BY FAM6, LINE, YEARMONTH, YEARMONTH_IPGO
            ORDER BY FAM6, LINE, YEARMONTH                
        )
)
 
SELECT X.PLANID, X.FAM1, X.FAM5, X.LINE, X.DESIGN_RULE, X.FAM6, X.VERSION, X.DR, SUBSTR(X.YEARMONTH,1,4) YEAR, X.YEARMONTH, X.NETDIE, Z.TG, X.EQ, WF_TTL, NVL(PKG_OUT_EQ/100000,0) PKG입고_억EQ, NVL(순입고_MEQ_FINAL/100,0) 순입고_억EQ
, WF_P_E
, NVL(ROUND((WF_TTL-WF_P_E)*X.NETDIE*Z.TG*X.EQ/100000000,7),0) 순생산_억EQ, TAT_BE
FROM (
    --26년 WF 생산 / PKG 입고
    SELECT PLANID, FAM1, FAM5, LINE, DESIGN_RULE, FAM6, VERSION, DR, YEARMONTH, NETDIE, EQ, SUM(WF_OUT) WF_TTL, SUM(PKG_OUT) PKG_OUT_CHIP, SUM(PKG_OUT*EQ) PKG_OUT_EQ, SUM(WF_P_E) WF_P_E
    FROM (
            SELECT  PLANID, FAM1, FAM5, P.LINE, DESIGN_RULE, P.FAM6_ADJ FAM6, VERSION, DR, YEARMONTH, NETDIE, EQ
                  , DECODE(GUBUN,'FAB_OUT',DECODE(ORDER_NUM,-3, PRE_M9, -2,PRE_M10,-1,PRE_M11,0,PRE_M12, 1,Y0M1, 2,Y0M2, 3, Y0M3, 4,Y0M4, 5,Y0M5, 6,Y0M6, 7,Y0M7, 8,Y0M8, 9,Y0M9, 10,Y0M10, 11,Y0M11, 12,Y0M12, 13, Y1M1, 14,Y1M2, 15, Y1M3, 16,Y1M4, 17,Y1M5, 18,Y1M6, 19,Y1M7, 20,Y1M8, 21,Y1M9, 22,Y1M10, 23,Y1M11, 24,Y1M12,0),0) WF_OUT
                  , DECODE(GUBUN,'WH_GUIDE',DECODE(ORDER_NUM,-3, PRE_M9, -2,PRE_M10,-1,PRE_M11,0,PRE_M12, 1,Y0M1, 2,Y0M2, 3, Y0M3, 4,Y0M4, 5,Y0M5, 6,Y0M6, 7,Y0M7, 8,Y0M8, 9,Y0M9, 10,Y0M10, 11,Y0M11, 12,Y0M12, 13, Y1M1, 14,Y1M2, 15, Y1M3, 16,Y1M4, 17,Y1M5, 18,Y1M6, 19,Y1M7, 20,Y1M8, 21,Y1M9, 22,Y1M10, 23,Y1M11, 24,Y1M12,0),0) PKG_OUT
                  , DECODE(GUBUN,'FAB_P_E',DECODE(ORDER_NUM,-3, PRE_M9, -2,PRE_M10,-1,PRE_M11,0,PRE_M12, 1,Y0M1, 2,Y0M2, 3, Y0M3, 4,Y0M4, 5,Y0M5, 6,Y0M6, 7,Y0M7, 8,Y0M8, 9,Y0M9, 10,Y0M10, 11,Y0M11, 12,Y0M12, 13, Y1M1, 14,Y1M2, 15, Y1M3, 16,Y1M4, 17,Y1M5, 18,Y1M6, 19,Y1M7, 20,Y1M8, 21,Y1M9, 22,Y1M10, 23,Y1M11, 24,Y1M12,0),0) WF_P_E
            FROM {new_table_name} P
            , (
                    SELECT YEARMONTH, ROW_NUMBER() OVER( ORDER BY YEARMONTH)-4 ORDER_NUM
                    FROM
                    (
                        SELECT DISTINCT FNCMONTH YEARMONTH FROM MST_FNC_TIME
                        WHERE FNCMONTH >= '202509'
                        AND FNCMONTH <= '202712'
                    )
            ) Q
            ,(
                  -- FAM6별 기준정보
                        SELECT FAM6, MAX(VERSION) VERSION, MAX(DR) DR, MAX(FAM1) FAM1, MAX(EQ) EQ, MAX(FAM5) FAM5, MAX(NETDIE) NETDIE
                        FROM  MEMBP_SDB.EXP_MST_BP_MASTER
                        WHERE WORK_FLAG ='Y'
                        GROUP BY FAM6                                                          

            ) R
            WHERE 1=1
            AND PLANID = :plan_id
            AND GUBUN IN ('FAB_OUT','FAB_P_E','WH_GUIDE')
            AND P.FAM6_ADJ = R.FAM6 (+)
    )
   GROUP BY PLANID, FAM1, FAM5, LINE, DESIGN_RULE, FAM6, VERSION, DR, YEARMONTH, NETDIE, EQ
) X
,(
        --순입고
        SELECT FAM1, FAM5, LINE, DESIGN_RULE, FAM6, VERSION, YEARMONTH_IPGO, 순입고_MEQ_ORG, TAT_BE
            , CASE WHEN FAM6 LIKE '%HBM%' THEN 순입고_MEQ_ORG  --HBM은 PKG입고= 순입고 삭제 (241024)
                   --WHEN (FAM6 LIKE '%VHG' AND YEARMONTH_IPGO LIKE '2024%') OR (FAM6 LIKE '%VHB' AND YEARMONTH_IPGO LIKE '2024%') OR (FAM6 LIKE '%VLB' AND YEARMONTH_IPGO LIKE '2024%') THEN 순입고_MEQ_ORG *0.8  --24년 D1B 모바일만 SBL 20%, 25년은 다 풀기
                   ELSE 순입고_MEQ_ORG END 순입고_MEQ_FINAL
        FROM (
                SELECT  A.FAM1, A.FAM5, A.LINE, A.DESIGN_RULE, A.FAM6, A.VERSION, YEARMONTH_IPGO, EQ, SUM(WF_TTL) WF_OUT, SUM(PKG_OUT) PKG입고_CHIP
                       , ROUND(SUM(NVL(ROUND(WF_OUT*IPGO_AVAIL_PORTION*NETDIE*TOTAL_GROSS*EQ,8)/1000000,0)),5) 순입고_MEQ_ORG 
                       , ROUND(SUM(PKG_OUT*EQ)/1000) PKG입고_MEQ, max(TAT_BE) TAT_BE
                       --A.FAM6, A.LINE,  A.VERSION, A.DR, A.YEARMONTH, A.WF_OUT, YEARMONTH_IPGO, IPGO_AVAIL_PORTION, TOTAL_GROSS, ROUND(WF_OUT*NETDIE*EQ*TOTAL_GROSS,8)/1000000 WF순생산,  ROUND(WF_OUT*NETDIE*EQ*TOTAL_GROSS*IPGO_AVAIL_PORTION,8)/1000000 기여
                FROM (
                       --24~25년 WF 생산 (PE 전환 제외)/ PKG 입고
 
            SELECT FAM1, FAM5, LINE, DESIGN_RULE, FAM6, VERSION, DR, YEARMONTH, NETDIE, EQ, SUM(WF_OUT) WF_TTL, SUM(WF_OUT-WF_ES_SAMPLE) WF_OUT, SUM(PKG_OUT) PKG_OUT
 
            FROM (
 
                    SELECT  FAM1, FAM5, LINE, DESIGN_RULE, FAM6_ADJ FAM6, VERSION, DR, YEARMONTH, NETDIE, EQ
                          , DECODE(GUBUN,'FAB_OUT',DECODE(ORDER_NUM,-3, PRE_M9, -2,PRE_M10,-1,PRE_M11,0,PRE_M12, 1,Y0M1, 2,Y0M2, 3, Y0M3, 4,Y0M4, 5,Y0M5, 6,Y0M6, 7,Y0M7, 8,Y0M8, 9,Y0M9, 10,Y0M10, 11,Y0M11, 12,Y0M12, 13, Y1M1, 14,Y1M2, 15, Y1M3, 16,Y1M4, 17,Y1M5, 18,Y1M6, 19,Y1M7, 20,Y1M8, 21,Y1M9, 22,Y1M10, 23,Y1M11, 24,Y1M12,0),0) WF_OUT
                          , DECODE(GUBUN,'WH_GUIDE',DECODE(ORDER_NUM,-3, PRE_M9, -2,PRE_M10,-1,PRE_M11,0,PRE_M12, 1,Y0M1, 2,Y0M2, 3, Y0M3, 4,Y0M4, 5,Y0M5, 6,Y0M6, 7,Y0M7, 8,Y0M8, 9,Y0M9, 10,Y0M10, 11,Y0M11, 12,Y0M12, 13, Y1M1, 14,Y1M2, 15, Y1M3, 16,Y1M4, 17,Y1M5, 18,Y1M6, 19,Y1M7, 20,Y1M8, 21,Y1M9, 22,Y1M10, 23,Y1M11, 24,Y1M12,0),0) PKG_OUT
                          , DECODE(GUBUN,'FAB_P_E',DECODE(ORDER_NUM,-3, PRE_M9, -2,PRE_M10,-1,PRE_M11,0,PRE_M12, 1,Y0M1, 2,Y0M2, 3, Y0M3, 4,Y0M4, 5,Y0M5, 6,Y0M6, 7,Y0M7, 8,Y0M8, 9,Y0M9, 10,Y0M10, 11,Y0M11, 12,Y0M12, 13, Y1M1, 14,Y1M2, 15, Y1M3, 16,Y1M4, 17,Y1M5, 18,Y1M6, 19,Y1M7, 20,Y1M8, 21,Y1M9, 22,Y1M10, 23,Y1M11, 24,Y1M12,0),0) WF_ES_SAMPLE
                    FROM {new_table_name} P
                     , (
                            SELECT YEARMONTH, ROW_NUMBER() OVER( ORDER BY YEARMONTH)-4 ORDER_NUM
                            FROM (
                                    SELECT DISTINCT FNCMONTH YEARMONTH FROM MST_FNC_TIME
                                    WHERE FNCMONTH >= '202509'
                                    AND FNCMONTH <= '202712'
                           )
 
                    ) Q
                     ,(
                          -- FAM6별 기준정보
                        SELECT FAM6, MAX(VERSION) VERSION, MAX(DR) DR, MAX(FAM1) FAM1, MAX(EQ) EQ, MAX(FAM5) FAM5, MAX(NETDIE) NETDIE
                        FROM  MEMBP_SDB.EXP_MST_BP_MASTER
                        WHERE WORK_FLAG ='Y'
                        GROUP BY FAM6             
                     ) R
 
                WHERE 1=1
                AND PLANID = :plan_id
                AND GUBUN IN ('FAB_OUT','FAB_P_E','WH_GUIDE')
                AND P.FAM6_ADJ = R.FAM6 (+)
            ) 
            GROUP BY FAM1, FAM5, LINE, DESIGN_RULE, FAM6, VERSION, DR, YEARMONTH, NETDIE, EQ
        ) A
         ,(
             -- 25년에 입고되는 비율
             SELECT  FAM6, LINE, YEARMONTH, YEARMONTH_IPGO, IPGO_AVAIL_PORTION, TAT_BE
             FROM IPGO_PORTION_2627
             WHERE 1=1 --FAM5 NOT LIKE '%HBM%'  --HBM은 PKG입고를 그대로 사용 삭제 (241024)

--             SELECT DISTINCT FAM5, YEARMONTH, YEARMONTH, 1 IPGO_AVAIL_PORTION

--             WHERE FAM5  LIKE '%HBM%'   
         ) B
         , (
             select  FAM6_ADJ FAM6, line, YEARMONTH
                    , DECODE(GUBUN,'YC',DECODE(ORDER_NUM,-3, PRE_M9, -2,PRE_M10,-1,PRE_M11,0,PRE_M12, 1,Y0M1, 2,Y0M2, 3, Y0M3, 4,Y0M4, 5,Y0M5, 6,Y0M6, 7,Y0M7, 8,Y0M8, 9,Y0M9, 10,Y0M10, 11,Y0M11, 12,Y0M12, 13, Y1M1, 14,Y1M2, 15, Y1M3, 16,Y1M4, 17,Y1M5, 18,Y1M6, 19,Y1M7, 20,Y1M8, 21,Y1M9, 22,Y1M10, 23,Y1M11, 24,Y1M12,0),0) TOTAL_GROSS
             from {new_table_name} a,
             (
                 SELECT YEARMONTH, ROW_NUMBER() OVER( ORDER BY YEARMONTH)-4 ORDER_NUM
                 FROM
                 (
                     SELECT DISTINCT FNCMONTH YEARMONTH FROM MST_FNC_TIME
                     WHERE FNCMONTH >= '202509'
                     AND FNCMONTH <= '202712'
                 )
             ) B
             WHERE 1=1
             and PLANID = :plan_id
             and GUBUN in ('YC')
          
         ) C
         WHERE A.YEARMONTH = B.YEARMONTH (+)
         AND A.FAM6 = B.FAM6 (+)
         AND A.LINE = B.LINE (+)
         AND A.FAM6 = C.FAM6
         AND A.LINE = C.LINE
         AND A.YEARMONTH = C.YEARMONTH
         AND A.FAM6 NOT LIKE 'ER%'
         GROUP BY  A.FAM1, A.FAM5, A.LINE, A.DESIGN_RULE, A.FAM6, A.VERSION, YEARMONTH_IPGO, EQ
         HAVING YEARMONTH_IPGO NOT LIKE '2025%'
         )

--AND X.FAM6 = Y.FAM6(+)
--AND X.LINE_CODE = Y.LINE_CODE(+)
--AND X.YEARMONTH_IPGO = Y.YEARMONTH(+)
--GROUP BY X.FAM1, X.DR, X.FAB_PRODUCT, X.FAM5, X.FAM6, X.LINE_CODE, YEARMONTH_IPGO, TAT_PORTION, FO_PE, NETDIE, EQ, TAT_EDSBE, TG,VERSION;



 
) Y
 ,(
         -- 수율
         SELECT  FAM6_ADJ FAM6, LINE, YEARMONTH
                , DECODE(GUBUN,'YC',DECODE(ORDER_NUM,-3, PRE_M9, -2,PRE_M10,-1,PRE_M11,0,PRE_M12, 1,Y0M1, 2,Y0M2, 3, Y0M3, 4,Y0M4, 5,Y0M5, 6,Y0M6, 7,Y0M7, 8,Y0M8, 9,Y0M9, 10,Y0M10, 11,Y0M11, 12,Y0M12, 13, Y1M1, 14,Y1M2, 15, Y1M3, 16,Y1M4, 17,Y1M5, 18,Y1M6, 19,Y1M7, 20,Y1M8, 21,Y1M9, 22,Y1M10, 23,Y1M11, 24,Y1M12,0),0) TG
         FROM {new_table_name} A,
         (
             SELECT YEARMONTH, ROW_NUMBER() OVER( ORDER BY YEARMONTH)-4 ORDER_NUM
             FROM
             (
                 SELECT DISTINCT FNCMONTH YEARMONTH FROM MST_FNC_TIME
                 WHERE FNCMONTH >= '202509'
                 AND FNCMONTH <= '202712'
             )
         ) B
         WHERE 1=1
         AND PLANID = :plan_id
         AND GUBUN IN ('YC')
         AND FAM6_ADJ NOT LIKE 'ER_%'
 ) Z
 WHERE X.FAM6 = Y.FAM6 (+)
 AND X.LINE = Y.LINE (+)
 AND X.YEARMONTH = Y.YEARMONTH_IPGO (+)
 AND X.FAM6 = Z.FAM6 (+)
 AND X.LINE = Z. LINE (+)
 AND X.YEARMONTH = Z.YEARMONTH (+)
 AND X.YEARMONTH NOT LIKE '2025%'
 --AND X.FAM5 NOT LIKE 'ER%'



"""

query2=f"""

SELECT FAM6, MAX(VERSION) VERSION, MAX(DR) DR, MAX(FAM1) FAM1, max(USER_FAM1) USER_FAM1, max(USER_FAM2) USER_FAM2 ,MAX(EQ) EQ, MAX(FAM5) FAM5, MAX(NETDIE) NETDIE
FROM  MEMBP_SDB.EXP_MST_BP_MASTER
WHERE WORK_FLAG ='Y'
GROUP BY FAM6

"""

## Summary 파일 관련 함수

df_line = pd.DataFrame({
    'LINE': ['4', '3','B','P','J','H','G','E','C','W','L','M'],
    'LINE2': ['P4L','P3L','P2L','P1L','17L','16L','15L','13L','12L','11L','X2L','X1L']
    })

df_dr = pd.DataFrame({
    'DESIGN_RULE':['D0A', 'D1D','D1C','D1B-F','D1B','D1A',
                    'D1Z-F','D1Z','D1Y-F','D1Y','D1X-F','D1X','D20','D25',
                    'V568','V430','V286','V236',
                    'V176','V133','V128C','V128','V92','FN14-B'],
    'DR2': ['D0A', 'D1D','D1C','D1B','D1B','D1A',
                    'D1Z','D1Z','D1Y','D1Y','D1X','D1X','D20','D25',
                    'V11','V10','V9','V8',
                    'V7','V6P','V6','V6','V5','FET-B']
    })

df_dr2 = pd.DataFrame({
    'DESIGN_RULE':['D0A', 'D1D','D1C','D1B-F','D1B','D1A',
                    'D1Z-F','D1Z','D1Y-F','D1Y','D1X-F','D1X','D20','D25',
                    'V568','V430','V286','V236',
                    'V176','V133','V128C','V128','V92','FN14-B'],
    'DR3': ['D0A', 'D1D','D1C','D1B','D1B','D1A',
                    'D1Z','D1Z','D1Y','D1Y','D1X','D1X','D20','D20',
                    'V11','V10','V9','V8',
                    'V7','V6P','V6','V6','V5','FET-B']
    })

order_line2=['17L','16L','15L','13L','12L','11L','P4L','P3L','P2L','P1L','X2L','X1L']

order_line3=['P4L','P3L','P2L','P1L','17L','16L','15L','13L','12L','11L','X2L','X1L']

order_dr2=['D1D','D1C','D1B','D1A',
            'D1Z','D1Y','D1X',
            'D20','D25',
            'V11','V10','V9','V8',
            'V7','V6P','V6','V5','FET-B']

## 라인 공정별 Pivot 함수
def make_df_pv(df,col_value='WF_TTL',order_line=order_line3,colsum_true=True):

    # 라인 공정별 Pivot
    df_pv=pd.pivot_table(
            df,         
            values=col_value,                        # 집계할 값
            index=['FAM1','LINE2','DR2'],            # 행으로 사용할 열 'FAM1' 'DESIGN_RULE' 'LINE'
            columns='YEARMONTH',          # 열로 사용할 열 'year' 'quater' 
            aggfunc='sum',            # 집계 함수 (여기서는 합계)
            fill_value=0,             # 결측값을 0으로 채우기      
            margins=True,
            observed=True               
        )
    
    # 합계 0인 데이터 삭제
    df_pv=df_pv[df_pv['All']!=0]

    df_pv = df_pv.drop(index='All', errors='ignore')  # 행 제거
    df_pv = df_pv.drop(columns='All', errors='ignore')  # 열 제거

    # 각 상위 그룹별 부분합 계산
    subtotals = df_pv.groupby(level=['FAM1', 'LINE2']).sum()
    subtotals.index = pd.MultiIndex.from_tuples([(idx[0], idx[1], '합계') for idx in subtotals.index])

    # 부분합을 먼저 추가하고 정렬하여 기존 순서를 유지
    df_pv = pd.concat([subtotals, df_pv])

    # 인덱스 순서를 유지하며 합계가 상위에 오도록 정렬
    df_pv = df_pv.sort_index(level=[0, 1, 2], ascending=[True, True, False])

    # 행 인덱스 데이터화
    df_pv=df_pv.reset_index()

    # df column명 변경
    df_pv.columns=['FAM1','LINE','DR']+df_pv.columns.to_list()[3:]

    # 데이터 순서 변경
    df_pv['LINE'] = pd.Categorical(df_pv['LINE'], categories=['합계']+order_line, ordered=True)
    df_pv['DR'] = pd.Categorical(df_pv['DR'], categories=['합계']+order_dr2, ordered=True)
    df_pv=df_pv.sort_values(by=['FAM1','LINE','DR'], ascending=[True, True, True]) 

    if colsum_true==True:
        # 2026과 2027년 컬럼 추출
        column_key=df_pv.columns.to_list()[:3]
        columns_2026 = [col for col in df_pv.columns if col.startswith('2026')]
        columns_2027 = [col for col in df_pv.columns if col.startswith('2027')]

        # 각 연도의 합 계산
        sum_2026 = df_pv[columns_2026].sum(axis=1)
        sum_2027 = df_pv[columns_2027].sum(axis=1)

        # 부분합을 삽입할 위치 설정
        df_pv['2026합'] = sum_2026
        df_pv['2027합'] = sum_2027

        # 2026합과 2027합을 중간에 삽입하여 새로운 데이터프레임 생성
        df_pv = pd.concat([df_pv[column_key],df_pv[columns_2026], df_pv[['2026합']], df_pv[columns_2027], df_pv[['2027합']]], axis=1)

    return df_pv


# 로깅 설정
#log_path='C:/Users/bo0612.kim/Desktop/GOC 교육/python코드/error_ebaTOgscmDB.log'
print('Current Working Dir:', os.getcwd())
log_path='./error_ebaTOgscmDB.log'

logging.basicConfig(filename=log_path, 
                    level=logging.ERROR, 
                    format='%(asctime)s\t%(levelname)s\t%(message)s')

# 1. Define input arguments
# ArgumentParser 객체 생성
parser = argparse.ArgumentParser(description='This script convert the EBA file to GSCM DB file.')

# 인수 정의
parser.add_argument('eba_edm_path', type=str, help='Path to the EBA EDM Path')
parser.add_argument('eba_filename', type=str, help='filename of EBA')
parser.add_argument('planid_val', type=str, help='value of new planid')

# 인수 파싱
args = parser.parse_args()

# 입력된 인수를 출력합니다.
print(f"eba_edm_path: {args.eba_edm_path}")
print(f"eba_filename: {args.eba_filename}")
print(f"planid_val: {args.planid_val}")

eba_path=args.eba_edm_path
eba_filename=args.eba_filename
planid_val=args.planid_val


# 2. EBA 파일 open 및 저장
# EBA 파일 열기
class CustomHttpClient(HttpClient):
    def get(self, url, params=None, **kwargs) -> requests.Response:
        return requests.get(
            url,
            params,
            proxies={
                "http": "http://12.26.204.100:8080",
                "https": "http://12.26.204.100:8080",
            },
            verify=False,
            **kwargs
        )


download_manager = WDMDownloadManager(CustomHttpClient())

WEBDRIVER_PATH = "C:/Servers/Selenium/drivers"
os.makedirs(WEBDRIVER_PATH, exist_ok=True)
options = EdgeOptions()
options.add_argument("--headless")

driver = webdriver.Edge(
    service=EdgeService(
        EdgeChromiumDriverManager(
            download_manager=download_manager,
            url = 'https://msedgedriver.microsoft.com',
            latest_release_url = 'https://msedgedriver.microsoft.com/LATEST_RELEASE'
        ).install()
    ), options=options
)

try:
    #driver.get("http://edm2.sec.samsung.net/cc/#/compact/verLink/172224209886704615/1")
    driver.get(eba_path)

    element = WebDriverWait(driver, timeout=5).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "div.btns span.r a:nth-child(2)")
						# (By.CSS_SELECTOR, "div.btns span.r a:nth-child(1)") # .xlsm 파일의 경우
            # (72oiC2cE3님)  xlsm은 편집, 보기 두개 메뉴만 있어서 1번째 엘리먼트로 선택 필요.
        )
    )
    assert "보기" == element.text
    element.click()
    time.sleep(5)
except Exception as e:
    logging.error("An unexpected error occurred: %s", e, exc_info=True)
finally:
    driver.quit()

try:
    # EBA 파일 - Simulation Sheet 연결
    app = xw.apps.active

    wb1 = [book for book in app.books if book.name.startswith(eba_filename)][0]
    print(wb1)

    sheet1 = [sheet for sheet in wb1.sheets if "Simulation" in sheet.name][0]
    print(sheet1)

    tmp = sheet1.range('BB1889').value
    print(tmp)

    # 데이터프레임 범위 찾기
    # A열 전체 범위 가져오기 (A열의 1부터 마지막 셀까지)
    column_a = sheet1.range('A:A').value

    start_row = None
    last_row = None

    for i, value in enumerate(column_a, start=1):
        if value is not None and value != "":
            start_row = i
            print(start_row)
            break

    for i, value in enumerate(column_a[::-1], start=1):
        if value is not None and value != "":
            last_row = len(column_a)-i+1
            print(last_row)
            break

    # 데이터가 들어있는 첫 번째 셀과 대각선 끝 셀 지정
    start_cell = 'BB'+str(start_row) # 첫 번째 데이터 셀 (예: B2)
    last_cell = 'CJ'+str(last_row)   # 대각선 끝 셀 (예: F10)

    # 지정된 범위에서 데이터 가져오기
    data_range = f"{start_cell}:{last_cell}"
    df = sheet1.range(data_range).options(pd.DataFrame, index=False, header=False).value

    wb1.close()

    # DataFrame 출력
    print(df.shape)
    print(df.head())

    # 필요 없는 열 삭제
    df.drop([33,34], axis=1, inplace=True)
    print(df.shape)

    # 컬럼명 생성
    df.columns=['FAM6','LINE', 'DESIGN_RULE', 'PROD_DESC','GUBUN',
                'PRE_M9', 'PRE_M10', 'PRE_M11',	'PRE_M12',
                'Y0M1', 'Y0M2',	'Y0M3', 'Y0M4', 'Y0M5', 'Y0M6', 
                'Y0M7', 'Y0M8', 'Y0M9',	'Y0M10', 'Y0M11', 'Y0M12',
                'Y1M1', 'Y1M2', 'Y1M3',	'Y1M4', 'Y1M5',	'Y1M6',	
                'Y1M7',	'Y1M8',	'Y1M9',	'Y1M10', 'Y1M11', 'Y1M12']
    print(df.head())

    # 필요 데이터 필터링
    gubun_val=['ASY_EOH',
            'CA','CE','CF','CT','C_TOT',
            'EDS_EOH',
            'FAB_EOH','FAB_GUIDE','FAB_IN', 'FAB_OUT', 'FAB_P_E',
            'GD',
            'STK_EOH','TOT_EOH','TST_EOH',
            'WH_ADJ_B','WH_AVAIL','WH_EOH','WH_EXE','WH_GUIDE','WH_STOCK','WORK_DAY',
            'YA','YC','YE','YF','YL','YT']

    df=df[df['GUBUN'].isin(gubun_val)].copy()
    df.reset_index(drop=True, inplace=True)

    df[['PRE_M9', 'PRE_M10', 'PRE_M11',	'PRE_M12']] = 0

    # 데이터에 None 값 있는지 확인
    non_cnt_col=df.isnull().sum()
    non_cnt_col[non_cnt_col>0]

    # PLANID 추가
    df['PLANID']=planid_val
    df = df[['PLANID'] + df.columns[:-1].tolist()]

    # FAM6 파일 EDM 말고 따로 Imort
    #fam6_path="C:/Users/bo0612.kim/Desktop/GOC 교육/python코드/240812_eba_fam6.xlsx"
    #fam6_path="C:/Users/bo0612.kim/Desktop/GOC 교육/python코드/240930_eba_fam6.xlsx"
    #fam6_path="D:/GOC 교육/python코드/241015_eba_fam6.xlsx"
    fam6_path="D:/GOC 교육/python코드/241212_eba_fam6.xlsx"

    app = xw.App(visible=False)
    wb = xw.Book(fam6_path)
    sheet = wb.sheets[0]
    df_fam6 = sheet.range('A1').options(pd.DataFrame, index=False, expand='table').value
    wb.close()

    # fam6 관련 columne 추가
    df=pd.merge(df, df_fam6, on='FAM6', how='left')
    print("GSCM Table")
    print(df)

    # GSCM gui_eba_2yr_test 생성
    #cx.init_oracle_client(lib_dir=r"C:\oracle\instantclient\instantclient_23_4")
    cx.init_oracle_client(lib_dir=r"C:\oracle\instantclient_23_5")
    
    dsn = cx.makedsn("gmgsdd09-vip.sec.samsung.net", 2541, service_name = "MEMSCM") # 오라클 주소
    conn = cx.connect(user="memscm", password="mem01scm", dsn=dsn, encoding="UTF-8") # 오라클 접속

    # 새로운 테이블이 존재하면 삭제하고, 새 테이블 생성
    delete_table_if_exists(new_table_name)

    # 테이블 구조 복사
    copy_table_structure(old_table_name, new_table_name)

    # 데이터프레임을 새로운 테이블에 삽입
    insert_dataframe_into_table(df, new_table_name)

    # 변수 값 정의
    params = {
        'plan_id': planid_val
        }
    print(params)

    # 오라클 테이블을 데이터 프레임으로 불러오기
    df_sunipgo = pd.read_sql(query, conn, params=params)
    print("sunipgo Table")
    print(df_sunipgo)

    df_info = pd.read_sql(query2, conn)
    print("info Table")
    print(df_info)    

    conn.close()

    #excel_path="C:/Users/bo0612.kim/Desktop/GOC 교육/python코드/ebaTOgscmdb.xlsx"
    excel_path1="./ebaTOgscmdb.xlsx"
    excel_path2="./ebaTOsunipgo.xlsx"

    ## C1에서부터 저장하는 방법
    with pd.ExcelWriter(excel_path1, engine='openpyxl') as writer:
        # 먼저 빈 엑셀 파일을 작성
        df.to_excel(writer, index=False, header=True, startrow=0, startcol=2)

    df_sunipgo.to_excel('df_sunipgo.xlsx', index=False)

    print_execution_time(start_time)

    # 3. Summary 관련 데이터 생성 및 저장

    ## 0) Summary 위한 컬럼 추가 및 Sorting
    df_sunipgo2 = df_sunipgo.merge(df_line, how='left', on='LINE')
    df_sunipgo2= df_sunipgo2.merge(df_dr, how='left', on='DESIGN_RULE')

    df_sunipgo2['LINE2'] = pd.Categorical(df_sunipgo2['LINE2'], categories=order_line3, ordered=True)
    df_sunipgo2['DR2'] = pd.Categorical(df_sunipgo2['DR2'], categories=order_dr2, ordered=True)
    df_sunipgo2=df_sunipgo2.sort_values(by=['FAM1','LINE2','DR2'], ascending=[True, True, True]) 
         

    ## 1) 라인공정별 WF Capa Summary

    ## ① 공정별 HBM 요약
    df_wf_hbm=pd.pivot_table(
            df_sunipgo2[df_sunipgo2['FAM6'].str.contains('HBM')],         
            values='WF_TTL',                        # 집계할 값
            index=['FAM1','DR2'],            # 행으로 사용할 열 'FAM1' 'DESIGN_RULE' 'LINE'
            columns='YEARMONTH',          # 열로 사용할 열 'year' 'quater' 
            aggfunc='sum',            # 집계 함수 (여기서는 합계)
            fill_value=0,             # 결측값을 0으로 채우기      
            margins=True,
            observed=True               
        )    

    df_wf_hbm=df_wf_hbm[df_wf_hbm['All']!=0]

    df_wf_hbm = df_wf_hbm.drop(index='All', errors='ignore')  # 행 제거
    df_wf_hbm = df_wf_hbm.drop(columns='All', errors='ignore')  # 열 제거

    ### 2026과 2027년 컬럼 추출
    columns_2026 = [col for col in df_wf_hbm.columns if col.startswith('2026')]
    columns_2027 = [col for col in df_wf_hbm.columns if col.startswith('2027')]

    ### 각 연도의 합 계산
    sum_2026 = df_wf_hbm[columns_2026].sum(axis=1)
    sum_2027 = df_wf_hbm[columns_2027].sum(axis=1)

    ### 부분합을 삽입할 위치 설정
    df_wf_hbm['2026합'] = sum_2026
    df_wf_hbm['2027합'] = sum_2027

    ### 2026합과 2027합을 중간에 삽입하여 새로운 데이터프레임 생성
    df_wf_hbm = pd.concat([df_wf_hbm[columns_2026], df_wf_hbm[['2026합']], df_wf_hbm[columns_2027], df_wf_hbm[['2027합']]], axis=1)

    df_wf_hbm=df_wf_hbm.reset_index()
    df_wf_hbm['FAM1']="HBM"
    df_wf_hbm

    ### ② Line/공정별 WF 
    df_wf_all=make_df_pv(df_sunipgo2,col_value='WF_TTL',order_line=order_line3,colsum_true=True)
    
    ### ③ Line/공정별 ER WF
    df_wf_er=make_df_pv(df_sunipgo2[df_sunipgo2['FAM6'].str.contains('ER')],col_value='WF_TTL',order_line=order_line3,colsum_true=False)
    
    ## 2) 라인공정별 순생산/순입고

    ### ① 순입고
    df_pv_sunipgo=make_df_pv(df_sunipgo2,col_value='순입고_억EQ',order_line=order_line3,colsum_true=True)

    ### ② 순생산 
    df_pv_sunsangsan=make_df_pv(df_sunipgo2,col_value='순생산_억EQ',order_line=order_line3,colsum_true=True)

    ## 3) 계수 정리
    df_sunipgo2= df_sunipgo2.merge(df_dr2, how='left', on='DESIGN_RULE')

    df_sunipgo2['DR3'] = pd.Categorical(df_sunipgo2['DR3'], categories=order_dr2, ordered=True)
    df_sunipgo2=df_sunipgo2.sort_values(by=['FAM1','LINE2','DR3'], ascending=[True, True, True]) 

    df_summary=pd.pivot_table(
                    df_sunipgo2,         
                    values=['WF_TTL','순입고_억EQ','순생산_억EQ','PKG입고_억EQ'],    # 집계할 값
                    index=['FAM1','DR3'],            # 행으로 사용할 열 'FAM1' 'DESIGN_RULE' 'LINE'
                    columns='YEAR',          # 열로 사용할 열 'year' 'quater' 
                    aggfunc='sum',            # 집계 함수 (여기서는 합계)
                    fill_value=0,             # 결측값을 0으로 채우기      
                    margins=False,
                    observed=True               
                )
    
    ### 사용자 정의 순서 (예: 먼저 B 그룹, 그 후 A 그룹)
    custom_order = [('WF_TTL', '2026'), ('순입고_억EQ', '2026'), ('순생산_억EQ', '2026'), ('PKG입고_억EQ', '2026'),
                    ('WF_TTL', '2027'), ('순입고_억EQ', '2027'), ('순생산_억EQ', '2027'), ('PKG입고_억EQ', '2027')]

    ### 컬럼 순서 재정렬
    df_summary = df_summary[custom_order]

    ### WF 1000나누기
    df_summary.loc[:, df_summary.columns.get_level_values(None).str.contains('WF_TTL', case=False)] /= 1000

    ### 각 상위 그룹별 부분합 계산
    subtotals = df_summary.groupby(level=['FAM1']).sum()
    subtotals.index = pd.MultiIndex.from_tuples([(idx[0], '합계') for idx in subtotals.index])

    ### 부분합을 먼저 추가하고 정렬하여 기존 순서를 유지
    df_summary = pd.concat([subtotals, df_summary])

    ### 순서 바꾸기
    df_summary=df_summary.swaplevel(0,1,axis=1)    

    ## 4) PKG 입고 관련
    ### ① Quater 컬럼 생성

    # yearmonth에서 뒤의 두 문자만 추출 (월)
    df_sunipgo2['MONTH'] = df_sunipgo2['YEARMONTH'].str[-2:].astype(int)

    # 월을 기준으로 분기(Quarter) 컬럼 생성
    def month_to_quarter(month):
        if 1 <= month <= 3:
            return 'Q1'
        elif 4 <= month <= 6:
            return 'Q2'
        elif 7 <= month <= 9:
            return 'Q3'
        elif 10 <= month <= 12:
            return 'Q4'
        else:
            return None

    df_sunipgo2['QUARTER'] = df_sunipgo2['MONTH'].apply(month_to_quarter)

    ### ② DRAM PKG
    df_pkg_dram=pd.pivot_table(
                        df_sunipgo2[(df_sunipgo2['FAM1']=='DRAM')],         
                        values=['PKG입고_억EQ'],    # 집계할 값
                        index=['FAM1','DR3'],       # 행으로 사용할 열 'FAM1' 'DESIGN_RULE' 'LINE'
                        columns=['YEAR','QUARTER'],          # 열로 사용할 열 'year' 'quater' 
                        aggfunc='sum',            # 집계 함수 (여기서는 합계)
                        fill_value=0,             # 결측값을 0으로 채우기      
                        margins=False,
                        observed=True               
                    )

    # 각 상위 그룹별 부분합 계산
    subtotals = df_pkg_dram.groupby(level=['FAM1'], observed=True).sum()
    subtotals.index = pd.MultiIndex.from_tuples([(idx[0], '합계') for idx in subtotals.index])

    # 부분합을 먼저 추가하고 정렬하여 기존 순서를 유지
    df_pkg_dram = pd.concat([subtotals, df_pkg_dram])

    df_pkg_dram.index = df_pkg_dram.index.set_levels(['DRAM_ALL',"DRAM"], level=0)

    ### ③ DRAM PKG HBM
    df_pkg_dram_hbm=pd.pivot_table(
                        df_sunipgo2[(df_sunipgo2['FAM1']=='DRAM')&(df_sunipgo2['FAM6'].str.contains('HBM'))],         
                        values=['PKG입고_억EQ'],    # 집계할 값
                        index=['FAM1','DR3'],       # 행으로 사용할 열 'FAM1' 'DESIGN_RULE' 'LINE'
                        columns=['YEAR','QUARTER'],          # 열로 사용할 열 'year' 'quater' 
                        aggfunc='sum',            # 집계 함수 (여기서는 합계)
                        fill_value=0,             # 결측값을 0으로 채우기      
                        margins=False,
                        observed=True               
                    )

    # 각 상위 그룹별 부분합 계산
    subtotals = df_pkg_dram_hbm.groupby(level=['FAM1'], observed=True).sum()
    subtotals.index = pd.MultiIndex.from_tuples([(idx[0], '합계') for idx in subtotals.index])

    # 부분합을 먼저 추가하고 정렬하여 기존 순서를 유지
    df_pkg_dram_hbm = pd.concat([subtotals, df_pkg_dram_hbm])

    df_pkg_dram_hbm.index = df_pkg_dram_hbm.index.set_levels(['HBM_ALL',"HBM"], level=0)

    ### 합치기
    df_pkg_dram = pd.concat([df_pkg_dram, df_pkg_dram_hbm])

    df_pkg_dram['2026']=df_pkg_dram.iloc[:, 0:4].sum(axis=1)
    df_pkg_dram['2027']=df_pkg_dram.iloc[:, 4:8].sum(axis=1)

    ### ③ USER FAM1/FAM2 상세 분석
    df_sunipgo2=pd.merge(df_sunipgo2, df_info[['FAM6','USER_FAM1','USER_FAM2']], on='FAM6', how='left')

    df_pkg_dram_userfam=pd.pivot_table(
                        df_sunipgo2[(df_sunipgo2['FAM1']=='DRAM')&((df_sunipgo2['USER_FAM1']!='HBM'))],         
                        values=['PKG입고_억EQ'],    # 집계할 값
                        index=['FAM1','USER_FAM1','USER_FAM2'],       # 행으로 사용할 열 'FAM1' 'DESIGN_RULE' 'LINE'
                        columns=['YEAR','QUARTER'],          # 열로 사용할 열 'year' 'quater' 
                        aggfunc='sum',            # 집계 함수 (여기서는 합계)
                        fill_value=0,             # 결측값을 0으로 채우기      
                        margins=False,
                        observed=True               
                    )

    ### 각 상위 그룹별 부분합 계산
    subtotals = df_pkg_dram_userfam.groupby(level=['FAM1', 'USER_FAM1'], observed=True).sum()
    subtotals.index = pd.MultiIndex.from_tuples([(idx[0], idx[1], '합계') for idx in subtotals.index])

    ### 부분합을 먼저 추가하고 정렬하여 기존 순서를 유지
    df_pkg_dram_userfam = pd.concat([subtotals, df_pkg_dram_userfam])

    order_user=['-','EDP','Mobile','Graphic']

    df_pkg_dram_userfam.index = df_pkg_dram_userfam.index.set_levels(
        pd.CategoricalIndex(df_pkg_dram_userfam.index.levels[1], categories=order_user, ordered=True),
        level=1
    )    

    ### 인덱스 순서를 유지하며 합계가 상위에 오도록 정렬
    df_pkg_dram_userfam = df_pkg_dram_userfam.sort_index(level=[0, 1, 2], ascending=[True, True, False])

    df_pkg_dram_userfam['2026']=df_pkg_dram_userfam.iloc[:, 0:4].sum(axis=1)
    df_pkg_dram_userfam['2027']=df_pkg_dram_userfam.iloc[:, 4:8].sum(axis=1)

    ### ④ USER FAM1/FAM2/DR 상세 분석
    df_pkg_dram_userfam2=pd.pivot_table(
                        df_sunipgo2[(df_sunipgo2['FAM1']=='DRAM')&(df_sunipgo2['USER_FAM2'].isin(['DDR5', 'LPDDR5']))],         
                        values=['PKG입고_억EQ'],    # 집계할 값
                        index=['FAM1','USER_FAM1','USER_FAM2','DR3'],       # 행으로 사용할 열 'FAM1' 'DESIGN_RULE' 'LINE'
                        columns=['YEAR','QUARTER'],          # 열로 사용할 열 'year' 'quater' 
                        aggfunc='sum',            # 집계 함수 (여기서는 합계)
                        fill_value=0,             # 결측값을 0으로 채우기      
                        margins=False,
                        observed=True               
                    )
    
    df_pkg_dram_userfam2['2026']=df_pkg_dram_userfam2.iloc[:, 0:4].sum(axis=1)
    df_pkg_dram_userfam2['2027']=df_pkg_dram_userfam2.iloc[:, 4:8].sum(axis=1)

    ### ⑤ Flash PKG
    df_pkg_flash=pd.pivot_table(
                        df_sunipgo2[(df_sunipgo2['FAM1']=='FLASH')&(df_sunipgo2['VERSION']!='-')],         
                        values=['PKG입고_억EQ'],    # 집계할 값
                        index=['FAM1','DR3','VERSION'],       # 행으로 사용할 열 'FAM1' 'DESIGN_RULE' 'LINE'
                        columns=['YEAR','QUARTER'],          # 열로 사용할 열 'year' 'quater' 
                        aggfunc='sum',            # 집계 함수 (여기서는 합계)
                        fill_value=0,             # 결측값을 0으로 채우기      
                        margins=False,
                        observed=True               
                    )

    order_ver=['AK', 'AJ', 'AI', 'IX', 'IL', 'IT',
            'BH', 'BU', 'BF',  'GQ', 'GF', 'GJ', 
            'CR', 'CU', 'EB', 'EJ', 'EQ','EK','DC']

    df_pkg_flash.index = df_pkg_flash.index.set_levels(
        pd.CategoricalIndex(df_pkg_flash.index.levels[2], categories=order_ver, ordered=True),
        level=2
    )

    # 각 상위 그룹별 부분합 계산
    subtotals = df_pkg_flash.groupby(level=['FAM1', 'DR3'], observed=True).sum()
    subtotals.index = pd.MultiIndex.from_tuples([(idx[0], idx[1], '합계') for idx in subtotals.index])

    subtotals2 = df_pkg_flash.groupby(level=['FAM1'], observed=True).sum()
    subtotals2.index = pd.MultiIndex.from_tuples([(idx[0], '합계', '합계') for idx in subtotals2.index])

    # 부분합을 먼저 추가하고 정렬하여 기존 순서를 유지
    df_pkg_flash = pd.concat([subtotals2, subtotals, df_pkg_flash])

    # 인덱스 순서를 유지하며 합계가 상위에 오도록 정렬
    df_pkg_flash = df_pkg_flash.sort_index(level=[0, 1, 2], ascending=[True, True, True])

    # 행 인덱스 데이터화
    df_pkg_flash=df_pkg_flash.reset_index()

    # df column명 변경
    df_pkg_flash.columns=['FAM1','DR','VERSION']+df_pkg_flash.columns.to_list()[3:]

    # 데이터 순서 변경
    df_pkg_flash['VERSION'] = pd.Categorical(df_pkg_flash['VERSION'], categories=['합계']+order_ver, ordered=True)
    df_pkg_flash['DR'] = pd.Categorical(df_pkg_flash['DR'], categories=['합계']+order_dr2, ordered=True)
    df_pkg_flash=df_pkg_flash.sort_values(by=['FAM1','DR','VERSION'], ascending=[True, True, True]) 

    df_pkg_flash['2026']=df_pkg_flash.iloc[:, 3:7].sum(axis=1)
    df_pkg_flash['2027']=df_pkg_flash.iloc[:, 7:11].sum(axis=1)

    ## 5) PSI 관련

    df_psi=pd.pivot_table(
                    df_sunipgo2[df_sunipgo2['YEAR'].str.contains('2027')],         
                    values=['WF_TTL','순생산_억EQ','순입고_억EQ','PKG입고_억EQ'],    # 집계할 값
                    index=['FAM1','DR2','VERSION'],            # 행으로 사용할 열 'FAM1' 'DESIGN_RULE' 'LINE'
                    columns='YEARMONTH',          # 열로 사용할 열 'year' 'quater' 
                    aggfunc='sum',            # 집계 함수 (여기서는 합계)
                    fill_value=0,             # 결측값을 0으로 채우기      
                    margins=False,
                    observed=True               
                )    
    
    ## 사용자 정의 순서 (예: 먼저 B 그룹, 그 후 A 그룹)
    measures=['WF_TTL','순생산_억EQ','순입고_억EQ','PKG입고_억EQ']
    dates=df_psi.columns.get_level_values('YEARMONTH').unique().values

    custom_order = [(measure, date) for measure in measures for date in dates]

    ## 컬럼 순서 재정렬
    df_psi = df_psi[custom_order]
   
    ## WF 1000나누기
    df_psi.loc[:, df_psi.columns.get_level_values(None).str.contains('WF_TTL', case=False)] /= 1000

    # WF 1000나누기
    df_dr_psi=df_psi.loc[:, df_psi.columns.get_level_values(None).str.contains('WF_TTL', case=False)].copy()
    df_dr_psi=df_dr_psi.groupby(level=['FAM1', 'DR2'], observed=True).sum()

    ## 6) 파일 저장
    excel_path3="./summary.xlsx"

    with pd.ExcelWriter(excel_path3, engine='xlsxwriter') as writer:
        # WF 시트
        # 첫 번째 데이터프레임을 'a' 시트에 저장
        df_wf_hbm.to_excel(writer, sheet_name='WF', index=False, startrow=0)
        
        # 두 번째 데이터프레임을 첫 번째 데이터프레임 바로 아래에 저장
        df_wf_all.to_excel(writer, sheet_name='WF', index=False, startrow=len(df_wf_hbm) + 2)

        # 세 번째 데이터프레임을 두 번째 데이터프레임 바로 아래에 저장
        df_wf_er.to_excel(writer, sheet_name='WF', index=False, startrow=len(df_wf_all) + len(df_wf_hbm) + 4)

        #################################################
        # sunipgo 시트
        # 순입고
        df_pv_sunipgo.to_excel(writer, sheet_name='Sunipgo', index=False, startrow=0) 
    
        # 순생산
        df_pv_sunsangsan.to_excel(writer, sheet_name='Sunipgo', index=False, startrow=len(df_pv_sunipgo) + 2)    
        #################################################

        # 계수 시트
        df_summary.to_excel(writer, sheet_name='계수', startrow=0) 

        #################################################
        # PKG입고 시트
        df_pkg_dram.to_excel(writer, sheet_name='PKG입고', startrow=0) 

        df_pkg_dram_userfam.to_excel(writer, sheet_name='PKG입고', startrow=len(df_pkg_dram) + 5) 

        df_pkg_dram_userfam2.to_excel(writer, sheet_name='PKG입고', startrow=len(df_pkg_dram)+len(df_pkg_dram_userfam) + 10) 

        df_pkg_flash.to_excel(writer, sheet_name='PKG입고', startrow=0, startcol=df_pkg_dram_userfam2.shape[1]+6) 

        #################################################
        # psi 시트
        df_psi.to_excel(writer, sheet_name='psi', startrow=1) 

        df_dr_psi.to_excel(writer, sheet_name='psi', startrow=1,startcol=df_psi.shape[1]+4)

    # 저장된 엑셀 파일을 불러오기 (openpyxl로 열기)
    wb = load_workbook(excel_path3)

    ws0 = wb['WF']
    # 특정 행과 열에 문자 입력 (예: 2행 A열에 'Hello' 입력)
    last_row_df = len(df_wf_all) + len(df_wf_hbm) + 4
    ws0[f'A{last_row_df}'] = 'ER'  

    ws = wb['Sunipgo']
    # 특정 행과 열에 문자 입력 (예: 2행 A열에 'Hello' 입력)
    last_row_df1 = len(df_pv_sunipgo) + 2
    ws[f'A{last_row_df1}'] = '순생산'     

    ws2 = wb['psi']
    start_col = 3  # C열
    for i in range(49):
        col_letter = get_column_letter(start_col + i)  # 열 문자(C, D, ..., AY)
        ws2[f'{col_letter}1'] = i + 1  # 1행에 숫자 입력

    start_col = 54  # C열
    for i in range(13):
        col_letter = get_column_letter(start_col + i)  # 열 문자(C, D, ..., AY)
        ws2[f'{col_letter}1'] = i + 1  # 1행에 숫자 입력

    wb.save(excel_path3)



except Exception as e:
    logging.error("An unexpected error occurred: %s", e, exc_info=True)
    print_execution_time(start_time)
