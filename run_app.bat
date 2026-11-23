@echo off
cd /d "%~dp0"
"%USERPROFILE%\anaconda3\envs\stock_analyzer\Scripts\streamlit.exe" run app.py
if errorlevel 1 pause
