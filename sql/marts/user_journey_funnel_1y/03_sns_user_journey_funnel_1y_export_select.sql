/* ============================================================================
   SNS 1-YEAR USER JOURNEY FUNNEL: CSV EXPORT SELECT
   MySQL 8.0

   PURPOSE
   - 최종 공개 마트를 CSV로 내보낼 때 사용하는 조회 SQL이다.
   - 출력 순서를 user_id로 고정해 재추출 결과를 비교하기 쉽게 한다.

   IMPORTANT
   - 현재 MySQL 서버는 secure_file_priv=NULL이므로 SELECT ... INTO OUTFILE을
     사용할 수 없다.
   - 따라서 이 SELECT를 MySQL 클라이언트에서 실행하고, 결과 스트림을
     export_mart_user_journey_funnel_1y.py가 UTF-8 BOM CSV로 저장한다.
   - 예상 결과: 668,461행, 58컬럼.
============================================================================ */

SET NAMES utf8mb4;

SELECT *
FROM votes_mart.mart_user_journey_funnel_1y
ORDER BY user_id;
