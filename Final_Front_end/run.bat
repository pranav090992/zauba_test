@echo off
start /B python "app.py"
timeout /t 2 >nul
start chrome "http://127.0.0.1:5000"