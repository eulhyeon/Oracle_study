SELECT T1.YYYYMM,
       T1.MS_ID,
       T1.SITE_ID,
       SUM(�����) AS ����Ǹŷ�,
       SUM(������) AS �����Ǹŷ�,
       SUM(�ﰳ��) AS �ֱ��Ǹŷ�,
       SUM(�б�) AS �б��Ǹŷ�,
       SUM(���б�) AS ���б��Ǹŷ�,
       SUM(����б�) AS ����б��Ǹŷ�,
       SUM(�����ֱ�) AS �����ֱ��Ǹŷ�,
       SUM(�����ֱ�) AS �����ֱ��Ǹŷ�,
       SUM(������) AS �������Ǹŷ�
FROM (
     SELECT :YYYYMM YYYYMM,
            T1.MS_ID,
            T1.SITE_ID,
            SUM(CASE WHEN T1.YYYYMM = TO_CHAR(TO_DATE(:YYYYMM, 'YYYYMM'),'YYYYMM') THEN T1.QUANTITY ELSE 0 END) AS �����,
            SUM(CASE WHEN T1.YYYYMM = TO_CHAR(ADD_MONTHS(TO_DATE(:YYYYMM, 'YYYYMM'),-1),'YYYYMM') THEN T1.QUANTITY ELSE 0 END) AS ������,
            SUM(CASE WHEN T1.YYYYMM BETWEEN TO_CHAR(ADD_MONTHS(TO_DATE(:YYYYMM, 'YYYYMM'), -2), 'YYYYMM') AND :YYYYMM THEN T1.QUANTITY ELSE 0 END) AS �ﰳ��,
            SUM(CASE WHEN T1.YYYY = SUBSTR(:YYYYMM,1,4) AND T1.QUATER = CEIL(CAST(SUBSTR(:YYYYMM,5,2) AS INT)/3) THEN T1.QUANTITY ELSE 0 END) AS �б�,
            SUM(CASE WHEN T1.YYYY = SUBSTR(:YYYYMM,1,4) AND T1.QUATER = (CEIL(CAST(SUBSTR(:YYYYMM,5,2) AS INT)/3)-1) THEN T1.QUANTITY ELSE 0 END) AS ���б�,
            SUM(CASE WHEN T1.YYYY = (CAST(SUBSTR(:YYYYMM,1,4) AS INT)-1) AND T1.QUATER = CEIL(CAST(SUBSTR(:YYYYMM,5,2) AS INT)/3) THEN T1.QUANTITY ELSE 0 END) AS ����б�,
            SUM(CASE WHEN T1.YYYY = SUBSTR(:YYYYMM,1,4) AND T1.MM BETWEEN '01' AND SUBSTR(:YYYYMM,5,2) THEN T1.QUANTITY ELSE 0 END) AS �����ֱ�,
            SUM(CASE WHEN T1.YYYY = (CAST(SUBSTR(:YYYYMM,1,4) AS INT)-1) AND T1.MM BETWEEN '01' AND SUBSTR(:YYYYMM,5,2) THEN T1.QUANTITY ELSE 0 END) AS �����ֱ�,
            SUM(CASE WHEN T1.YYYYMM = TO_CHAR(ADD_MONTHS(TO_DATE(:YYYYMM, 'YYYYMM'),-1),'YYYYMM') THEN T1.QUANTITY ELSE 0 END)
            OVER(PARTITION BY T1.YYYYMM, T1.CUST_ID, T1.MS_ID, T1.SITE_ID) AS ������
     FROM (
            SELECT T1.YYYYMM,
                   T1.MS_ID,
                   T1.SITE_ID,
                   T1.QUANTITY,
                   T1.CUST_ID,
                   SUBSTR(T1.YYYYMM,1,4) AS YYYY,
                   SUBSTR(T1.YYYYMM,5,2) AS MM,
                   CEIL(CAST(SUBSTR(T1.YYYYMM,5,2) AS INT)/3) AS QUATER
            FROM FD_DP_MNLY_SHIPMNT T1
            WHERE T1.YYYYMM BETWEEN '201807' AND '201906'
          ) T1
     WHERE 1=1
     GROUP BY YYYYMM,
              T1.MS_ID,
              T1.SITE_ID
    ) T1
WHERE 1=1
GROUP BY T1.YYYYMM,
         T1.MS_ID,
         T1.SITE_ID;
         
SELECT YYYYMM, MS_ID, SITE_ID, QUANTITY
FROM fd_dp_mnly_shipmnt
WHERE YYYYMM = '201812' AND MS_ID = '08P220';