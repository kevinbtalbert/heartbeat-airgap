# Copyright 2023 Cloudera, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# PROJECT HEARTBEAT FOR AIRGAPPED CLUSTERS
# VERSION 1.0
# ------------------------------------------------
# Cluster Name
# Full Cluster unique identifier
# Cloudera Manager Full Version
# Cloudera Runtime Full Version 
# List of Parcels on Cluster
# UUID portion of the license
# Total nodes under management in each cluster
# Total Number of logical cores in each cluster
# Total Size of memory in each cluster
# Total allocated storage for HDFS in each cluster
# Total allocated storage for Ozone in each cluster
# Date/Time of report creation

import cm_client
import requests
import datetime
import csv
import time
import json
import configparser

config = configparser.ConfigParser()
config.read('heartbeat_airgapped.conf')

# Configure HTTPS authentication
cm_client.configuration.username = config['DEFAULT']['cm_username']
cm_client.configuration.password = config['DEFAULT']['cm_password']
cm_client.configuration.verify_ssl = False
cm_host = config['DEFAULT']['cm_host']
ozone_recon_host = config['DEFAULT']['ozone_recon_host']

# Construct base URL for API
if config['DEFAULT']['https_yes_no'] == 'yes':
    cm_api_host = 'https://' + cm_host
    ozone_api_host = 'https://' + ozone_recon_host
    cm_port = '7183'
    ozone_port = '9889'

elif config['DEFAULT']['https_yes_no'] == 'no':
    cm_api_host = 'http://' + cm_host
    ozone_api_host = 'http://' + ozone_recon_host
    cm_port = '7180'
    ozone_port = '9888'

cm_api_version = 'v30'
if config['DEFAULT']['override_cm_api_version'] != 'no':
    cm_api_version = config['DEFAULT']['override_cm_api_version']
ozone_api_version = 'v1'
cm_api_url = cm_api_host + ':' + cm_port + '/api/' + cm_api_version
ozone_api_url = ozone_api_host + ':' + ozone_port + '/api/' + ozone_api_version
api_client = cm_client.ApiClient(cm_api_url)

cluster_api_instance = cm_client.ClustersResourceApi(api_client)
cm_api_instance = cm_client.ClouderaManagerResourceApi(api_client)
parcels_api_instance = cm_client.ParcelsResourceApi(api_client)
hosts_api_instance = cm_client.HostsResourceApi(api_client)
services_api_instance = cm_client.ServicesResourceApi(api_client)
hdfs_roles_api = cm_client.RolesResourceApi(api_client)
timeseries_api_instance = cm_client.TimeSeriesResourceApi(api_client)

clusters_api_response = cluster_api_instance.read_clusters(view='FULL')
cm_version = cm_api_instance.get_version()
hosts = hosts_api_instance.read_hosts()


def convert_b_to_gb(b):
    return (b / (1024 ** 3))

clusters = {}
output = []
ozone_yes_no = 'NO'
## Get general cluster metdata, per cluster
for cluster in clusters_api_response.items:
    clusters[cluster.name] = cluster.display_name

    # Cluster name, UUID, and version
    output.append(['Display Name', cluster.display_name])
    output.append(['Cluster Name', cluster.name])
    output.append(['Cluster UUID', cluster.uuid])
    output.append(['Cluster Runtime Version', cluster.full_version])
    output.append([])

    # Get activated parcels/versions
    for parcel in parcels_api_instance.get_parcel_usage(cluster.display_name).parcels:
        if parcel.activated == True:
            output.append(['Parcel Name / Version (Activated)', str(parcel.parcel_ref.parcel_name + ' ' + parcel.parcel_ref.parcel_version)])
    output.append([])

    ## Get HDFS/Ozone storage metrics
    storage_response = services_api_instance.read_services(cluster.name)
    for service in storage_response.items:
        if service.type == 'HDFS':
            hdfs_service = service
        if service.type == 'OZONE':
            ozone_service = service
            ozone_yes_no = 'YES'
    from_time = datetime.datetime.fromtimestamp(time.time() - 180000)
    to_time = datetime.datetime.fromtimestamp(time.time())
    query = "select dfs_capacity, dfs_capacity_used_non_hdfs, dfs_capacity_used WHERE category = SERVICE"
    result = timeseries_api_instance.query_time_series(_from=from_time, query=query, to=to_time)
    ts_list = result.items[0]
    for ts in ts_list.time_series:
        for point in ts.data:
            output.append([str(ts.metadata.attributes['entityName'] + " " + ts.metadata.metric_name), str(convert_b_to_gb(point.value)) + " GiB"])
            break
    output.append([])

## Ozone Parsing and API
if ozone_yes_no == 'YES':
    resp = requests.get(ozone_api_url + '/clusterState', verify=False)
    if resp.status_code == 200:
        resp_conv_json = json.loads(resp.text)
        output.append(["Ozone storage capacity", str(convert_b_to_gb(resp_conv_json["storageReport"]["capacity"])) + " GiB"])
        output.append(["Ozone storage used", str(convert_b_to_gb(resp_conv_json["storageReport"]["used"])) + " GiB"])
        output.append(["Ozone storage remaining", str(convert_b_to_gb(resp_conv_json["storageReport"]["remaining"])) + " GiB"])
        output.append([])

## Hosts Count, Cores, and Memory
total_hosts = 0
total_cores = 0
total_phys_cores = 0
total_mem = 0
for host in hosts.items:
    if host.commission_state == "COMMISSIONED":
        total_hosts += 1
        total_cores += host.num_cores
        total_phys_cores += host.num_physical_cores
        total_mem += host.total_phys_mem_bytes

output.append(['Total Number of Hosts (Commissioned only)', total_hosts])
output.append(['Total Logical Cores (Commissioned only)', total_cores])
output.append(['Total Physical Cores (Commissioned only)', total_phys_cores])
output.append(['Total Physical Memory (Commissioned only)', str(convert_b_to_gb(total_mem)) + " GiB"])
output.append([])

# CM Version and Report date/time
output.append(['Cloudera Manager Version', cm_version.version])
output.append(['Capture Date/Time', str(datetime.datetime.now())])

# Export to csv
with open((cluster.display_name + '_' + str(datetime.datetime.now()) + '.csv'), 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(output)