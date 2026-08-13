@echo off
setlocal

cd /d "%~dp0"

if exist "venv\Scripts\activate.bat" (
  call "venv\Scripts\activate.bat"
)

echo === METRO: public.posts ===
metro run tasks\stackoverflow_posts.yaml --secret-provider local --log-level DEBUG
if errorlevel 1 (
  echo Falha na task stackoverflow_posts
  exit /b 1
)

echo === METRO: public.comments ===
metro run tasks\stackoverflow_comments.yaml --secret-provider local --log-level DEBUG
if errorlevel 1 (
  echo Falha na task stackoverflow_comments
  exit /b 1
)

echo === Concluido ===
exit /b 0
