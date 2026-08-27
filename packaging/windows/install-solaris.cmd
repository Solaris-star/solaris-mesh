@echo off
setlocal
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0install-solaris.ps1" -SourceDirectory "%~dp0." %*
exit /b %ERRORLEVEL%
