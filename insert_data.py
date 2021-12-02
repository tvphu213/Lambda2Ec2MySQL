import paramiko
import boto3

region = 'us-west-2'
instances = ['i-085a6f191e9d0ea77']


def insert_to_sql(json_data):
    commands = []
    data = process_data(json_data)
    commands.append("cp template.sh insert_to_employee.sh")
    commands.append("sed -i 's/replace_string/" +
                    data+"/g' insert_to_employee.sh")
    commands.append("bash insert_to_employee.sh '"+data+"'")
    commands.append("rm insert_to_employee.sh")

    # boto3 client
    client = boto3.client("ec2", region_name=region)
    s3_client = boto3.client("s3")

    # getting instance information
    describeInstance = client.describe_instances(InstanceIds=instances)

    hostPublicIP = []
    # fetchin public IP address of the running instances
    for i in describeInstance["Reservations"]:
        for instance in i["Instances"]:
            if instance["State"]["Name"] == "running":
                hostPublicIP.append(instance["PublicIpAddress"])

    print(hostPublicIP)

    # downloading pem filr from S3
    s3_client.download_file("phutv12-lambda-authorite",
                            "app-key-pair-89.pem", "/tmp/file.pem")

    # reading pem file and creating key object
    key = paramiko.RSAKey.from_private_key_file("/tmp/file.pem")
    # an instance of the Paramiko.SSHClient
    ssh_client = paramiko.SSHClient()
    # setting policy to connect to unknown host
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    host = hostPublicIP[0]
    print("Connecting to : " + host)

    try:
        # connecting to server
        ssh_client.connect(hostname=host, username="ubuntu", pkey=key)
        print("Connected to :" + host)

        # Execute a command(cmd) after connecting/ssh to an instance
        for command in commands:
            print("running command: {}".format(command))
            stdin, stdout, stderr = ssh_client.exec_command(command)
            print(stdout.read())
            print(stderr.read())

        # close the client connection once the job is done
        ssh_client.close()
        return

    except Exception as e:
        print(e)


def process_data(json_data):
    result = "insert into employee (fullName, account, dateOfBirth, department, role, Address) values "
    strs = []
    for obj in json_data['employee']:
        str = "("
        lst = []
        for key in obj:
            lst.append('"' + obj[key] + '"')

        str += ", ".join(lst) + ")"
        strs.append(str)
    result += ", ".join(strs)
    result += ";"
    return result
