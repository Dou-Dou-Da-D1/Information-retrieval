@echo off

REM Set the current directory to the directory of the batch file
cd /d %~dp0

REM Start Elasticsearch
start "" "D:\Software\ElasticSearch\elasticsearch-7.14.0\bin\elasticsearch.bat"