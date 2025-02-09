@echo off
echo Git 상태 확인:
git status
echo.

echo 모든 변경 사항 추가:
git add .
echo.

echo 커밋 메시지와 함께 커밋:
set /p commitMessage="커밋 메시지를 입력하세요: "
git commit -m "%commitMessage%"
echo.

echo 원격 저장소에 푸시:
git push origin main

echo 완료!
pause