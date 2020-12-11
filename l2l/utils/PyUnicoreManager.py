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
    #Different access require a different token and the way to call a job
    ACCESS_JUDOOR=0
    ACCESS_COLLAB=1

    @staticmethod
    def generateToken(judooraccount,juddorpassword):
        return b64encode(b"judooraccount:juddorpassword").decode("ascii")

    @staticmethod
    def arrayToString(array):
        return ' '.join(map(str, array))

    @staticmethod
    def generate_L2L_launcher(variables, launcher):
        filename = "experiment.sh"
        try:
            print("Generating experiment for L2L", filename)

            new_local_path = os.path.join(variables["destiny_project_path"],
                                          variables["destiny_relative_subfolder"])
            # What the script does
            with open(os.path.join(variables["local_path"], filename), 'w') as file:
                file.write(
                'cd L2L ;\n' +
                'source env/bin/activate ;\n' +
                    'python /p/project/cslns/collab/L2L/bin/'+ launcher +' 2>experiment_stderr$(date "+%Y%m%d") 1>experiment_stdout$(date "+%Y%m%d") ;\n'+
                'deactivate ;\n'+
                'echo done!')
#                'cd L2L/bin ;\n' +
            # Steps to launch the script
            steps = Utils.arrayToString([" cd " +new_local_path+";",
                    "chmod 764 " + filename + ";",
                    "bash " + filename + ";",
                    "echo done!"])
            return filename, steps
        except Exception as e:
            raise e

    @staticmethod
    def generate_L2L_installation(variables):
        filename = "installation.sh"
        try:
            print ("Generating installation for L2L", filename)

            # What the script does
            with open(os.path.join(variables["local_path"], filename), 'w') as file:
                file.write('#!/bin/bash \n' +
                    'git clone https://github.com/aperezmartin/L2L.git ; \n'+
                    'cd L2L ;\n' +
                    'io_err=installation_stderr$(date "+%Y%m%d"); io_out=installation_stdout$(date "+%Y%m%d");'+
                    'python3 -m venv env;\n' +
                    'source env/bin/activate ;\n' +
                        'pip3 install -r requirements.txt 2>$io_err 1>$io_out;\n'+
                        'pip3 install http://apps.fz-juelich.de/jsc/jube/jube2/download.php?version=latest 2>$io_err 1>$io_out;\n'+
                        'python3 setup.py install 2>$io_err 1>$io_out;\n'+
                    'deactivate ;\n' +
                    'today=$(date "+%Y-%m-%d %H:%M:%S") ;\n'+
                    '/bin/rm -f '+filename+' 2>$io_err 1>$io_out;\n'+
                    'echo "$today - '+filename+' finished successfully! " >> installation_control$(date "+%Y%m%d") ;\n'+
                    'echo done!')

            # Steps to launch the script
            steps = Utils.generate_steps_bashscript(variables, filename)

            return filename, steps
        except Exception as e:
            raise e

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

                # set of instructions to launch the bash file
                return [" cd " + variables["destiny_working_path"] + ";",
                      "chmod 764 "+filename+";",
                      "./"+filename+";",
                      "chmod 764 " + variables["destiny_scriptname"]]
        except Exception as e:
            raise e

    @staticmethod
    def generate_PythonCompiler(variables):
        #TODO: set values
        #local_path, destiny_working_path, destiny_project_path, destiny_user_account, destiny_scriptname
        return [" cd " + variables["destiny_project_path"] + ";",
                "date >> text; echo done!"]

    @staticmethod
    def generate_steps_bashscript(variables, filename):
        return Utils.arrayToString([" cd " + variables["destiny_project_path"] + ";",
                    "chmod 764 " + filename + ";",
                    "bash " + filename + ";",
                    "echo done!"])

class Environment_UNICORE():

    def __init__(self,token,methodToAccess, serverToConnect, local_path,
                 destiny_project_path,destiny_relative_subfolder,
                 **args):

        self.urls,self.conn_info,self.job_info,self.script_info = {},{},{},{}

        # Required for a sever connection
        self.conn_info["token"] = token
        self.conn_info["methodToAccess"] = methodToAccess
        self.conn_info["serverToConnect"] = serverToConnect
        if "serverToRegister" in args.keys():
            self.conn_info["serverToRegister"] = args["serverToRegister"]
        else:
            self.conn_info["serverToRegister"] ="https://zam2125.zam.kfa-juelich.de:9112/HBP/rest/registries/default_registry"

        # URLs for the client side
        self.urls["local_path"] = local_path

        # URLs for the server side
        self.urls["destiny_project_path"] = destiny_project_path
        self.urls["destiny_relative_subfolder"] = destiny_relative_subfolder
        self.urls["destiny_parent_project"] = self.urls["destiny_project_path"].split('/')[-1]
        self.urls["destiny_server_endpoint"] = args["destiny_server_endpoint"] if "destiny_server_endpoint" in args.keys() else None
        self.urls["destiny_libraries_path"] = args["destiny_libraries_path"] if "destiny_libraries_path" in args.keys() else None

        # Parameter for the Job execution
        self.job_info["serverArgs"]= args["serverArgs"] if "serverArgs" in args.keys() else {}

        # Script to run on the server
        self.script_info["language"] = args["language"] if "language" in args.keys() else "python"
        self.script_info["name"] =  args["script_name"] if "script_name" in args.keys() else None
        self.script_info["parameters"] = args["script_parameters"] if "script_parameters" in args.keys() else None
        self.script_info["needcompiler"] = args["needcompiler"] if "needcompiler" in args.keys() else True

class PyUnicoreManager(object):
    def __init__(self, environment, verbose=False,  **keyword_args):
        self.verbose = verbose
        self.env = environment

        #Accesing with JUDOOR or COLLAB token have have different parameters in the PyUnicore.Transport
        # oidc=False doesnt work with collab token
        self.transport = None
        if Utils.ACCESS_JUDOOR == self.env.conn_info["methodToAccess"]:
            self.transport = unicore_client.Transport(self.env.conn_info["token"], oidc=False)
        elif Utils.ACCESS_COLLAB == self.env.conn_info["methodToAccess"]:
            self.transport = unicore_client.Transport(self.env.conn_info["token"])

        self.registry = unicore_client.Registry(self.transport, self.env.conn_info["serverToRegister"])
        self.site = self.registry.site(self.env.conn_info["serverToConnect"])
        self.client = unicore_client.Client(self.transport, self.site.site_url)

        # Get the object Storage
        for obj in self.site.get_storages():
            if obj.storage_url.endswith(self.env.urls["destiny_server_endpoint"]):
                self.storage = obj
                break

        if self.env.conn_info["token"] is None:
            print("Token is required!")

        if 'trajectory' in keyword_args:
            self.trajectory = keyword_args['trajectory']

    def getStorage(self):
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

    def createJob(self, list_of_steps, job_args):
        executable = ""
        if(self.verbose):
            print("Executing commands...")
            for item in list_of_steps.split(";"):
                print(" -", item)
            print("")

        job = {}
        job['Executable'] = list_of_steps

        #No job type means launching job in the Bach System
        if "jobType" in self.env.job_info["serverArgs"].keys():
            job['Job type'] = self.env.job_info["serverArgs"]["jobType"] #"interactive"

        # data stage in - TBD
        job['Imports'] = []

        # data stage out - TBD
        job['Exports'] = []

        # Resources - TBD
        if "Resources" in self.env.job_info["serverArgs"].keys():
            job['Resources'] = self.env.job_info["serverArgs"]["Resources"]
        else:
            job['Resources'] = []

        return job

    """
    TODO: Adapt again to the stan executions
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

        """

    def __run_job(self,job):
        cmd_job = self.client.new_job(job_description=job)

        # Wait until the job finishes
        print("Job status...", cmd_job.properties['status'])
        cmd_job.poll()
        print('Job finished!')

        result_job = {}
        wd = cmd_job.working_dir
        result_job["stderr"] = [x.decode('utf8') for x in wd.stat("/stderr").raw().readlines()]
        result_job["stdout"] = [x.decode('utf8') for x in wd.stat("/stdout").raw().readlines()]
        return cmd_job, result_job

    def __uploadFiles(self, filesToUpload, relative_subfolder):
        if len(filesToUpload) == 0:
            raise "Nothing to upload"

        # Uploading files
        print("Uploading to ", self.storage.storage_url)

        #destination="collab/filename"
        for filename in filesToUpload:
            self.storage.upload(os.path.join(self.env.urls["local_path"], filename),
                           destination=os.path.join(relative_subfolder, filename))# it works like this ../PROJECT/ "collab/filename"
            print(" - Uploaded file", filename)

    def __download(self, filesToDownload, relative_subfolder):
        if len(filesToDownload) == 0:
            raise "Nothing to download"

        print("Downloading from ", self.storage.storage_url)
        for filename in filesToDownload:
            remote = self.storage.stat(os.path.join(relative_subfolder,filename))# it works like this ../PROJECT/ "collab/filename"
            remote.download(os.path.join(self.env.urls["local_path"],filename))
            print(" - Downloaded file", filename)

    def one_run(self, steps):
        if len(steps) == 0:
            print("No instructions to execute")
            return

        #Executing a job
        job = self.createJob(list_of_steps=steps,job_args={})
        cmd_job, result_job = self.__run_job(job)
        return result_job

    #experiment_name is "lfl-fun-ga-py"
    #experiment.sh call "python experiment_name"
    def run_flow_L2L(self,experiment_name, filesToUpload=[],filesToDownload=[], setIntallation=False):
        try:
            start_time = time.time()

            print("Connected to", str(self.env.conn_info["serverToConnect"]) + "!")
            parent_project = self.env.urls["destiny_parent_project"]
            if setIntallation:
                # Create & Upload installation.sh in the server
                filename, steps =  Utils.generate_L2L_installation(variables=self.env.urls)
                self.__uploadFiles([filename],
                                   relative_subfolder=parent_project) #wikicollab

                # Launch the installation.sh in the server
                job = self.createJob(list_of_steps=steps, job_args={})
                cmd_job, result_job = self.__run_job(job)

            # Create launcher.py., it will call the experiment i.e."python l2l-fun-ga.py"
            launcher_file, steps = Utils.generate_L2L_launcher(variables=self.env.urls,
                                                            launcher=experiment_name)#relative_subfolder="collab/L2L/bin"

            #To upload for example: launcher.sh, experiment.py and optimizee.py
            filesToUpload.append(launcher_file)
            self.__uploadFiles(filesToUpload,
                               relative_subfolder=os.path.join(parent_project, self.env.urls["destiny_relative_subfolder"]))

            #run experiment
            # Launch the installation.sh in the server
            job = self.createJob(list_of_steps=steps, job_args={})
            cmd_job, result_job = self.__run_job(job)

            #Download
            self.__download(filesToDownload,
                               relative_subfolder=os.path.join(parent_project,
                                                               self.env.urls["destiny_relative_subfolder"]))
            print("Flow time --- %s seconds ---" % round(time.time() - start_time,3))
            return result_job
        except Exception as e:
            print("General error in run_flow_L2L", e)

################

"""
Exmaple: Stand
env = Environment_UNICORE(token=mytoken,
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

"""
# Exmaple: Python
env = Environment_UNICORE(token=mytoken,
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
"""


"""Notes:
"destiny_project_path" is used for installation
"destiny_relative_subfolder" is used to uploadfiles inside of the project and run scripts there
"""

"""
#Example: to place it into Collab
mytoken = ""
env = Environment_UNICORE(token=mytoken,
                 methodToAccess= Utils.ACCESS_JUDOOR,
                 serverToConnect="JUSUF",
                 destiny_server_endpoint="PROJECT",
                 serverArgs={ 'Resources': {"Nodes" : "1","Runtime" : "10"},
                              'jobType': "interactive"},
                 local_path=os.path.join(os.environ['HOME'], "temp_PyUnicore"),
                 destiny_project_path="/p/project/cslns/collab",
                 destiny_relative_subfolder="L2L/bin")
py = PyUnicoreManager(environment = env, verbose=False)

#Single job
#result = py.one_run(steps="cd /p/project/cslns/collab;date >> text;cat text")
#for line in result["stdout"]:
#    print(line)

#L2L job flow
#Write your optimizee & store it
optimizee = os.path.join(os.environ['HOME'], "temp_PyUnicore","myoptimizee.py")
optzee_path, optzee_filename = os.path.split(optimizee)


#write your experiment
# call an external optimizee or internal of L2L
experiment_name = "l2l-fun-ga.py"

job_result = py.run_flow_L2L(experiment_name=experiment_name,
                         filesToUpload=[optzee_filename],
                         filesToDownload=["experiment_stderr20201209","experiment_stdout20201209"],
                         setIntallation=True)# First time "setIntallation" is mandatory
print(job_result)
"""