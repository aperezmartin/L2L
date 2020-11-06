from base64 import b64encode
import pyunicore.client as unicore_client
import json
import os

def showSources(client):
    storages = client.get_storages()
    for s in storages:
        print(s)

def showJobs(client, tag):
    for j in client.get_jobs():
        print(j)
        if j.properties['status'].lower() == tag.lower():  # 'successful'
            print('Job:', j.job_id, ' Status:', j.properties['status'])
            print('Working_dir:', j.working_dir)

def showSites(transport):
    sites = unicore_client.get_sites(transport)
    for k, v in sites.items():
        print(k, '->', v)

def testJobDescription(list_of_steps):
    job = {}

    executable = ""
    print("Executing...")
    for item in list_of_steps:
        print(" -", item)
        executable += item
    print("")

    job['Executable'] = executable
    job['Job type'] = "interactive"

    # data stage in - TBD
    job['Imports'] = []

    # data stage out - TBD
    job['Exports'] = []

    # Resources - TBD
    job['Resources'] = {}

    return job

def generateBashCompiler(local_path):
    filename = "compilator.sh"
    try:
        print("Generating Compiler...")
        newfile = os.path.join(local_path, filename)

        #the tabular space is because to generate the right format of bash file(.sh)
        with open(newfile, 'w') as file:
            file.write('''\
#!/bin/bash

current_dir=$(pwd)
project=$1
judoor_user=$2
options="STAN_OPENCL=TRUE" #not included for now
scriptToCompile=$3

#Prepare variables
cmdstan_dir=$project/$judoor_user/cmdstan
model_dir=$cmdstan_dir/scripts/$scriptToCompile

echo compiling models ...
cd $cmdstan_dir && make $model_dir && cd $current_dir
echo done!
			''')
            print(" - Created", filename)
    except Exception as e:
        raise e

""" Explanation
Precondition: 
 - cmdstan has to be cloned and compiled in the server, I've used 2.25
 - access to /p/project/cslns/
 - judoor account with token

Connect to the server
Generate a compiler sh for Stan script
Upload everything to the server
Change permissions on files and compile the Stan script
Wait until the job finish
"""
try:
    #TODO: set a dynamic token connected to the judoor_account.
    token = b64encode(b"judooraccount:juddorpassword").decode("ascii")
    base_url = "https://zam2125.zam.kfa-juelich.de:9112/JUSUF/rest/core/"

    # Get the connection
    transport = unicore_client.Transport(token, oidc=False)
    registry = unicore_client.Registry(transport, unicore_client._HBP_REGISTRY_URL)

    # Set senv variables for the Server
    path_home = "/p/project/cslns/perezmartin1/"
    path_stan = "/p/project/cslns/perezmartin1/cmdstan"
    path_stan_scripts = "/p/project/cslns/perezmartin1/cmdstan/scripts/"

    source_path = os.path.join(os.environ['HOME'], "temp_PyUnicore")
    destiny_path = "perezmartin1/cmdstan/scripts"
    base_url_project = "https://zam2125.zam.kfa-juelich.de:9112/JUSUF/rest/core/storages/PROJECT/"

    # Generate compiler
    destiny_project = "/p/project/cslns"
    destiny_user_account = "perezmartin1"
    destiny_scriptname = "Seeghorseshoe2"

    generateBashCompiler(source_path)

    # Loading local files
    print("Loading local folder..", source_path)
    filesToUpload = os.listdir(source_path)
    for file in filesToUpload:
        print(" - Loaded file", file)
    print("")

    # Uploading files to the server
    storage = unicore_client.Storage(transport, base_url_project)
    print("Uploading...", destiny_path)
    for file in filesToUpload:
        storage.upload(os.path.join(source_path, file), destination=os.path.join(destiny_path, file))
        print(" - Uploaded file", file)

    # Run a job
    obj_client = unicore_client.Client(transport=transport, site_url=base_url)
    executable = ["cd " + path_stan_scripts + ";",
                  "chmod 764 compilator.sh;",
                  "./compilator.sh " + destiny_project + " " + destiny_user_account + " " + destiny_scriptname + ";",
                  "chmod 764 " + destiny_scriptname + ";"]

    # I've tried with the option "STAN_OPENCL=TRUE" but the generate *.o.
    # TODO: add options in the compilation time to speed up the execution
    # executable = [" cd " + path_stan + ";",
    #				  " make STAN_OPENCL=TRUE ./scripts/" + scriptname + ";",
    #				  " cd " + path_home + ";",
    #				  " python3 script.py;"]

    job = testJobDescription(list_of_steps=executable)
    cmd_job = obj_client.new_job(job_description=job)

    # Wait until the job finishes
    print(cmd_job.properties['status'])
    cmd_job.poll()
    print('Job finished!')

    # Download result
    # TODO: pending to know what is the output to be download.


except Exception as e:
    print(e)
    raise e
