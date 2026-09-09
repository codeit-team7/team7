param(
    [string]$MySqlUser = 'root',
    [string]$MySqlHost = 'localhost',
    [int]$MySqlPort = 3306
)

$ErrorActionPreference = 'Stop'
try { $Host.UI.RawUI.WindowTitle = 'V2 RATE PRECISION REPAIR - ENTER PASSWORD' } catch { }

$mysqlExe = 'C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe'
$bundleDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repairSql = (Join-Path $bundleDir '02z_rate_precision_repair_v2.sql').Replace('\', '/')
$repairLog = Join-Path $bundleDir 'last_rate_precision_repair_v2.log'

$freeSpaceGB = [math]::Round((Get-PSDrive -Name C).Free / 1GB, 1)
Write-Host "현재 C드라이브 여유 공간: $freeSpaceGB GB"
if ($freeSpaceGB -lt 10) {
    throw 'C드라이브 여유 공간이 10GB 미만이라 정밀도 복구를 시작하지 않습니다.'
}

Write-Host 'Stage 1 수락률 소수점 정밀도만 복구합니다.'
Write-Host '친구요청 상세 원장과 기존 7개 마트는 다시 만들거나 변경하지 않습니다.'
Write-Host 'MySQL 비밀번호를 입력하세요.'

& $mysqlExe `
    --host=$MySqlHost `
    --port=$MySqlPort `
    --user=$MySqlUser `
    -p `
    --default-character-set=utf8mb4 `
    --show-warnings `
    --execute="SOURCE $repairSql;" 2>&1 | Tee-Object -FilePath $repairLog

if ($LASTEXITCODE -ne 0) {
    throw "정밀도 복구가 중단되었습니다. 종료 코드: $LASTEXITCODE"
}

Write-Host '정밀도 복구가 끝났습니다. 이 창은 닫아도 됩니다.'
