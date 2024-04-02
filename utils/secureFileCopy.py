import paramiko

# create ssh client
ssh_client = paramiko.SSHClient()

# remote server credentials
host = "hostname"
username = "username"
password = "password"
port = "port"

ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(hostname=host, port=port, username=username, password=password)

ftp = ssh_client.open_sftp()
files = ftp.get("remote_file_path", "local_file_path")

# close the connection
ftp.close()
ssh_client.close()
