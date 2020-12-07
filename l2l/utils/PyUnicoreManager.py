import pickle

import pyunicore.client as unicore_client
from base64 import b64encode
import os, time, logging
from enum import Enum
import logging.config

from l2l.logging_tools import create_shared_logger_data, configure_loggers
from l2l.paths import Paths
from l2l.utils.trajectory import Trajectory

logger = logging.getLogger("utils.Environment_UNICORE")

class Utils():

    @staticmethod
    def generateToken(judooraccount,juddorpassword):
        return b64encode(b"judooraccount:juddorpassword").decode("ascii")

    @staticmethod
    def generate_StandCompiler_BashFile(variables):
        filename = "compilator.sh"
        try:
            print("Generating Stan Compiler...")
            newfile = os.path.join(variables["local_path"], filename)

            with open(newfile, 'w') as file:
                file.write('#!/bin/bash \n' +
                    'current_dir=$(pwd)'+
                    'project="'+variables["destiny_project_path"]+'"'+
                    'judoor_user="'+variables["destiny_user_account"]+'"'+
                    '#options="STAN_OPENCL=TRUE" #not included for now'+
                    'scriptToCompile="'+variables["destiny_scriptname"]+'"' +

                    '#Prepare variables'+
                    'cmdstan_dir=$project/$judoor_user/cmdstan'+
                    'model_dir=$cmdstan_dir/scripts/$scriptToCompile'+

                    'echo compiling models ...'+
                    'cd $cmdstan_dir && make $model_dir && cd $current_dir'+
                    'echo done!')
                print(" - Created", filename)
                print(" - Created steps")

                return [" cd " + variables["destiny_working_path"] + ";",
                      "chmod 764 compilator.sh;",
                      "./compilator.sh;",
                      "chmod 764 " + variables["destiny_scriptname"]]
        except Exception as e:
            raise e

    @staticmethod
    def generate_PythonCompiler(variables):
        #local_path, destiny_working_path, destiny_project_path, destiny_user_account, destiny_scriptname
        return [" cd " + variables["destiny_project_path"] + ";",
                "date >> text; echo done"]

class Environment_UNICORE():

    def __init__(self,token, serverToConnect, local_path,
                 destiny_working_path, destiny_project_path, destiny_libraries_path,
                 **args):
        #keyword_args['filename']
        self.urls = dict()
        self.conn_info= dict()
        self.script_info = dict()

        # Required for a sever connection
        self.conn_info["token"] = token
        self.conn_info["serverToConnect"] = serverToConnect

        # URLs for the client side
        self.urls["local_path"] = local_path

        # URLs for the server side
        #self.urls["destiny_working_path"] = destiny_working_path #For downloading
        #self.urls["destiny_scriptname"] = destiny_scriptname
        #self.urls["destiny_user_account"] = destiny_user_account
        self.urls["destiny_project_path"] = destiny_project_path
        #self.urls["destiny_project_url"] = destiny_project_url
        self.urls["destiny_libraries_path"] = destiny_libraries_path

        # Script to run on the server
        self.script_info["language"] = args["language"] if "language" in args.keys() else "python"
        self.script_info["name"] =  args["script_name"] if "script_name" in args.keys() else None
        self.script_info["parameters"] = args["script_parameters"] if "script_parameters" in args.keys() else None
        self.script_info["needcompiler"] = args["needcompiler"] if "needcompiler" in args.keys() else True


class PyUnicoreManager(object):
    def __init__(self, environment,  **keyword_args):
        self.env = environment
        self.transport = unicore_client.Transport(self.env.conn_info["token"], oidc=False)
        self.client = unicore_client.Client(self.transport, self.env.conn_info["serverToConnect"])

        if self.env.conn_info["token"] is None:
            print("Token is required!")

        if 'trajectory' in keyword_args:
            self.trajectory = keyword_args['trajectory']

    def getSesources(self):
        return self.client.get_storages()

    def getJobs(self, status=None):
        result=[]
        list = self.client.get_jobs()
        if status:
            for j in list:
                if j.properties['status'].lower() == status.lower():
                    result.append(j)
            return result
        else:
            return list

    def getSites(self):
        return unicore_client.get_sites(self.transport)

    def createJob(self, list_of_steps):
        job = {}

        executable = ""
        print("Executing commands...")
        for item in list_of_steps:
            print(" -", item)
            executable += item
        print("")

        job['Executable'] = "cd /p/project/cslns/collab; date >> text ;echo done"#executable


        #No job type means launching job in the Bach System
        #job['Job type'] = "interactive"

        # data stage in - TBD
        job['Imports'] = []

        # data stage out - TBD
        job['Exports'] = []

        # Resources - TBD
        job['Resources'] = {}
        #    "Nodes" : "1",
        #    "Runtime" : "10",
        #}

        return job

    def run(self, stepsToExcute):

        # List of step to execute the job
        executable = str()

        # Generate the compiler and return list of command to run it on the server. Before uploading files
        if self.env.script_info["needcompiler"]:
            steps=""
            if self.env.script_info["language"] == "stan":
                steps = Utils.generate_StandCompiler_BashFile(self.env.urls)
            elif self.env.script_info["language"] == "python":
                steps = Utils.generate_PythonCompiler(self.env.urls)
            executable += ' '.join(map(str, steps))
        else:
            executable += ' '.join(map(str, stepsToExcute))

        # Loading local files
        #print("Loading local folder..", self.env.urls["local_path"])
        #filesToUpload = os.listdir(self.env.urls["local_path"])
        #for file in filesToUpload:
        #    print(" - Loaded file", file)

        #print("exit")
        #exit(1)
        # Uploading files to the server
        #storage = unicore_client.Storage(self.transport, self.env.urls["destiny_project_url"])
        #print("Uploading...", self.env.urls["destiny_working_path"])
        #for file in filesToUpload:
        #    storage.upload(os.path.join(self.env.urls["local_path"], file), destination=os.path.join(self.env.urls["destiny_working_path"], file))
            #print(" - Uploaded file", file)


        # Add steps to the job execution



        # Run a job
        if len(executable) == 0:
            print("No instructions to execute")
            return

        print("Executing a job...")
        job = self.createJob(list_of_steps=executable)
        cmd_job = self.client.new_job(job_description=job)

        # Wait until the job finishes
        print(cmd_job.properties['status'])
        cmd_job.poll()
        print('Job finished!')

        wd = cmd_job.working_dir
        dict= {}
        dict["stderr"] = [x.decode('utf8') for x in wd.stat("/stderr").raw().readlines()]
        dict["stdout"] = [x.decode('utf8') for x in wd.stat("/stdout").raw().readlines()]
        return dict

    def one_run(self, executable, filesToUpload=[]):
        if len(executable) == 0:
            print("No instructions to execute")
            return

        print("Executing a job...")
        job = self.createJob(list_of_steps=executable)
        cmd_job = self.client.new_job(job_description=job, inputs=filesToUpload)

        # Wait until the job finishes
        print("Status...", cmd_job.properties['status'])
        cmd_job.poll()
        print('Job finished!')

        wd = cmd_job.working_dir
        dict = {}
        dict["stderr"] = [x.decode('utf8') for x in wd.stat("/stderr").raw().readlines()]
        dict["stdout"] = [x.decode('utf8') for x in wd.stat("/stdout").raw().readlines()]
        return dict

################
"""
Exmaple: Stand
env = Environment_UNICORE(token="cGVyZXptYXJ0aW4xOmFwbUAzMDQwNTA",
                 serverToConnect="https://zam2125.zam.kfa-juelich.de:9112/JUSUF/rest/core/",
                 local_path=os.path.join(os.environ['HOME'], "temp_PyUnicore"),
                 destiny_working_path="perezmartin1/cmdstan/scripts",
                 destiny_scriptname="Seeghorseshoe2",
                 destiny_user_account="perezmartin1",
                 destiny_project_path="/p/project/cslns",
                 destiny_project_url="https://zam2125.zam.kfa-juelich.de:9112/JUSUF/rest/core/storages/PROJECT/",
                 destiny_libraries_path="/p/project/cslns/perezmartin1/cmdstan/",
                 script_language="python",
                 script_name="",
                 script_parameters="",
                 needcompiler=True)
py = PyUnicoreManager(environment = env)
result = py.run()
print(result)
"""

# Single job + results
env = Environment_UNICORE(token="cGVyZXptYXJ0aW4xOmFwbUAzMDQwNTA",
                 serverToConnect="https://zam2125.zam.kfa-juelich.de:9112/JUSUF/rest/core/",
                 local_path=os.path.join(os.environ['HOME'], "temp_PyUnicore"),
                 destiny_working_path="./",
                 destiny_project_path="/p/project/cslns/collab",
                 destiny_project_url="https://zam2125.zam.kfa-juelich.de:9112/JUSUF/rest/core/storages/PROJECT/",
                 destiny_libraries_path="/p/project/cslns/collab/")
py = PyUnicoreManager(environment = env)
file = os.path.join(os.environ['HOME'], "temp_PyUnicore","test.py")
result = py.one_run(executable=["cd /p/project/cslns/collab; ls -l >> text; echo done"],filesToUpload=[])
print(result)

