/* Stage 1: 사용자·학교·친구요청·학교 확산 */
SELECT '[1/2] 사용자·학교 기준정보 구축 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/01_user_acquisition_and_school_bridge_v2.sql;
SELECT '[1/2] 사용자·학교 기준정보 완료' AS build_progress;

SELECT '[2/2] 친구요청·학교 확산 구축 시작' AS build_progress;
SOURCE C:/Users/lucy5/Desktop/team7/sql/marts/v2_complete_coverage/02_friend_request_and_school_viral_v2.sql;
SELECT '[2/2] Stage 1 완료' AS build_progress;
