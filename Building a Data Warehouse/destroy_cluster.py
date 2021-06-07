import pandas as pd
import boto3
import json
import psycopg2

from botocore.exceptions import ClientError
import configparser


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


def destroy_redshift_cluster(redshift):
    """
    Destroy the Redshift cluster (request deletion)
    :param redshift: The Redshift client resource
    :return:None
    """
    global DWH_CLUSTER_IDENTIFIER
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)

def aws_client(service, region):
    """
    Creates an AWS client
    :param service: The service
    :param region: The region of the service
    :return:
    """
    global KEY, SECRET
    return boto3.client(service, aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name=region)

def main():
    parse_config_file()

    redshift = aws_client('redshift', "us-west-2")

    if check_cluster_creation(redshift):
        print('available')
        destroy_redshift_cluster(redshift)
        print('New redshift cluster status: ')
        print(get_redshift_cluster_status(redshift))
    else:
        print('notyet')


if __name__ == '__main__':
    main()
