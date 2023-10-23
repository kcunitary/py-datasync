# /bin/bash
pyinstaller server.py -p .
cp -r config dist/server
mkdir dist/server/logs
cd dist
tar -zcvf  server.tar.gz server
