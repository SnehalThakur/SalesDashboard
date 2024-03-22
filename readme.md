
1. Install FastApi
pip install fastapi

2. Install ASGI server
pip install "uvicorn[standard]"

pip install passlib


3. Run app
uvicorn main:app --reload

sudo apt-get update
sudo apt install python3-pip
python3 -m pip install pip --upgrade
pip install pyopenssl --upgrade
pip install -U urllib3 requests
pip3 install -r requirements.txt

sudo apt install nginx

sudo nano /etc/nginx/sites-available/fastapi_nginx

sudo service nginx restart


git status

git commit -m "Commit msg"

git push -u origin master




pem windows 

PS C:\Users\snehal\PycharmProjects\BizwareDashboard\com\bizware\resources> icacls.exe dashboard_ubuntu.pem /reset
processed file: dashboard_ubuntu.pem
Successfully processed 1 files; Failed processing 0 files

PS C:\Users\snehal\PycharmProjects\BizwareDashboard\com\bizware\resources> icacls.exe dashboard_ubuntu.pem /grant:r "$($env:username):(r)"
processed file: dashboard_ubuntu.pem
Successfully processed 1 files; Failed processing 0 files

PS C:\Users\snehal\PycharmProjects\BizwareDashboard\com\bizware\resources> icacls.exe dashboard_ubuntu.pem /inheritance:r
processed file: dashboard_ubuntu.pem
Successfully processed 1 files; Failed processing 0 files


ssh -i "C:\Users\snehal\PycharmProjects\BizwareDashboard\com\bizware\resources\dashboard_ubuntu.pem" ubuntu@ec2-16-170-232-248.eu-north-1.compute.amazonaws.com

ngrok http 8000 --host-header="localhost:8000"