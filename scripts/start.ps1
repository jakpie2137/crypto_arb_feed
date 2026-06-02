@echo off
setlocal
cd /d %~dp0..
if not exist demo-config.json copy demo-config.example.json demo-config.json
if not exist secrets\pg_password.txt echo postgres> secrets\pg_password.txt
python scripts\render_env_from_config.py --docker-env-file .env.generated
docker compose up -d --build %*
endlocal
