param(
    [string]$MySqlUser = 'root',
    [string]$MySqlHost = 'localhost',
    [int]$MySqlPort = 3306
)

$ErrorActionPreference = 'Stop'
try { $Host.UI.RawUI.WindowTitle = 'STOP MYSQL BUILD - ENTER PASSWORD' } catch { }
$mysqlExe = 'C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe'
$bundleDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$cleanupSql = (Join-Path $bundleDir 'stop_orphan_build_v2.sql').Replace('\', '/')

Write-Host '구형 친구관계 생성 쿼리만 안전하게 중단합니다.'
Write-Host 'MySQL 비밀번호를 입력하세요.'
& $mysqlExe `
    --host=$MySqlHost `
    --port=$MySqlPort `
    --user=$MySqlUser `
    -p `
    --default-character-set=utf8mb4 `
    --show-warnings `
    --execute="source $cleanupSql"

if ($LASTEXITCODE -ne 0) {
    throw "정리 요청이 실패했습니다. 종료 코드: $LASTEXITCODE"
}
Write-Host '정리 요청이 끝났습니다. 이 창은 닫아도 됩니다.'
