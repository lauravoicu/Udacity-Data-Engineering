import pandas as pd
import boto3
import json
import psycopg2

from botocore.exceptions import ClientError
import configparser

KEY                    = None
SECRET                 = None

DWH_CLUSTER_TYPE       = None
DWH_NUM_NODES          = None
DWH_NODE_TYPE          = None

DWH_CLUSTER_IDENTIFIER = None
DWH_DB                 = None
DWH_DB_USER            = None
DWH_DB_PASSWORD        = None
DWH_PORT               = None

DWH_IAM_ROLE_NAME      = None

def parse_config_file():
    """
    Parse the dwh.cfg configuration file
    :return:
    """
    global KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, \
        DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, \
        DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME

    print("Parsing the config file...")
    config = configparser.ConfigParser()
    with open('dwh.cfg') as configfile:
        config.read_file(configfile)

        KEY = config.get('AWS', 'KEY')
        SECRET = config.get('AWS', 'SECRET')

        DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
        DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
        DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

        DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
        DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")

        DWH_DB = config.get("DWH","DWH_DB")
        DWH_DB_USER = config.get("DWH","DWH_DB_USER")
        DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
        DWH_PORT = config.get("DWH","DWH_PORT")


def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    '''
    Creates IAM Role for Redshift, to allow it to use AWS services
    '''

    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        
        
    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)
    return roleArn


def create_cluster(redshift, roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD):
    '''
    Creates Redshift cluster
    '''

    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)

def get_redshift_cluster_status(redshift):
    """
    Retrieves the Redshift cluster status
    :param redshift: The Redshift resource client
    :return: The cluster status
    """
    global DWH_CLUSTER_IDENTIFIER
    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    cluster_status = cluster_props['ClusterStatus']
    return cluster_status.lower()


def check_cluster_creation(redshift):
    """
    Check if the cluster status is available, if it is returns True. Otherwise, false.
    :param redshift: The Redshift client resource
    :return:bool
    """
    if get_redshift_cluster_status(redshift) == 'available':
        return True
    return False


def write_cluster_infos(redshift):
    """
    Write back to the dwh.cfg configuration file the cluster endpoint and IAM ARN
    :param redshift: The redshift resource client
    :return:
    """
    global DWH_CLUSTER_IDENTIFIER
    print("Writing the cluster address and IamRoleArn to the config file...")

    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    config = configparser.ConfigParser()

    with open('dwh.cfg') as configfile:
        config.read_file(configfile)

    config.set("CLUSTER", "HOST", cluster_props['Endpoint']['Address'])
    config.set("IAM_ROLE", "ARN", cluster_props['IamRoles'][0]['IamRoleArn'])

    with open('dwh.cfg', 'w+') as configfile:
        config.write(configfile)


def open_ports(ec2, redshift):
    '''
    Update clusters security group to allow access through redshift port
    '''

    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    try:
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
            )
    except Exception as e:
        print(e)


def main():
    parse_config_file()

    df = pd.DataFrame({"Param":
                    ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                "Value":
                    [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
                })

    print(df)


    ec2 = boto3.resource('ec2',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                    )

    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )

    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    roleArn = create_iam_role(iam, DWH_IAM_ROLE_NAME)
    
    create_cluster(redshift, roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD)
    if check_cluster_creation(redshift):
        print('available')
        print(get_redshift_cluster_status(redshift))
    else:
        print('notyet')

    write_cluster_infos(redshift)

    open_ports(ec2, redshift)

    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Connected')

    conn.close()


if __name__ == "__main__":
    main()