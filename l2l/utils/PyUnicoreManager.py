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
    def generate_StandCompiler_BashFile(local_path, destiny_working_path, destiny_project_path, destiny_user_account, destiny_scriptname):
        filename = "compilator.sh"
        try:
            print("Generating Stan Compiler...")
            newfile = os.path.join(local_path, filename)

            # the tabular space is because to generate the right format of bash file(.sh)
            with open(newfile, 'w') as file:
                file.write('''\
#!/bin/bash

current_dir=$(pwd)
project='''+destiny_project_path+'''
judoor_user='''+destiny_user_account+'''
options="STAN_OPENCL=TRUE" #not included for now
scriptToCompile='''+destiny_scriptname+'''

#Prepare variables
cmdstan_dir=$project/$judoor_user/cmdstan
model_dir=$cmdstan_dir/scripts/$scriptToCompile

echo compiling models ...
cd $cmdstan_dir && make $model_dir && cd $current_dir
echo done!
        			''')
                print(" - Created", filename)
                print(" - Created steps")

                return ["cd " + destiny_working_path + ";",
                      "chmod 764 compilator.sh;",
                      "./compilator.sh;",
                      "chmod 764 " + destiny_scriptname + ";"]
        except Exception as e:
            raise e

    @staticmethod
    def generate_PythonCompiler():
        return []

class Environment_UNICORE():
    def __init__(self, token, serverToConnect, local_path, destiny_working_path, destiny_scriptname, destiny_user_account, destiny_project_path, destiny_project_url, destiny_libraries_path, script_language, script_name, script_parameters, needcompiler):
        self.urls = dict()
        self.conn_info= dict()
        self.script_info = dict()

        # Required for a sever connection
        self.conn_info["token"] = token
        self.conn_info["serverToConnect"] = serverToConnect

        # URLs for the client side
        self.urls["local_path"] = local_path

        # URLs for the server side
        self.urls["destiny_working_path"] = destiny_working_path
        self.urls["destiny_scriptname"] = destiny_scriptname
        self.urls["destiny_user_account"] = destiny_user_account
        self.urls["destiny_project_path"] = destiny_project_path
        self.urls["destiny_project_url"] = destiny_project_url
        self.urls["destiny_libraries_path"] = destiny_libraries_path

        # Script to run on the server
        self.script_info["language"] = script_language
        self.script_info["name"] = script_name
        self.script_info["parameters"] = script_parameters
        self.script_info["requirecompiler"]= needcompiler

        # Trajectory of each experiment
        self.trajectory_info = dict()
        self.trajectory_exec = None


    def createTrayectory(self):
        self.trajectory = Trajectory(name=self.trajectory_info["trajectory_name"])

class PyUnicoreManager(object):
    def __init__(self, environment,  **keyword_args):
        self.env = environment
        self.transport = unicore_client.Transport(self.env.conn_info["token"], oidc=False)
        self.client = unicore_client.Client(self.transport, self.env.conn_info["serverToConnect"])

        #Set up
        #print("URLS:")
        #for key, value in self.env.urls.items():
        #    print(" -", key, '->', value)

        if self.env.conn_info["token"] is None:
            print("Token is required!")

        if 'trajectory' in keyword_args:
            self.trajectory = keyword_args['trajectory']

        "exec"


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

    def run(self):

        # List of step to execute the job
        executable = str()

        # Generate the compiler and return list of command to run it on the server. Before uploading files
        if self.env.script_info["requirecompiler"]:
            steps=""
            if self.env.script_info["language"] == "stan":
                steps = Utils.generate_StandCompiler_BashFile(local_path=self.env.urls["local_path"],
                                                                   destiny_working_path=self.env.urls["destiny_working_path"],
                                                                   destiny_project_path=self.env.urls["destiny_project_path"],
                                                                   destiny_user_account=self.env.urls["destiny_user_account"],
                                                                   destiny_scriptname=self.env.urls["destiny_scriptname"])
            elif self.env.script_info["language"] == "python":
                steps = Utils.generate_PythonCompiler()
            executable += ' '.join(map(str, steps))

        # Loading local files
        print("Loading local folder..", self.env.urls["local_path"])
        filesToUpload = os.listdir(self.env.urls["local_path"])
        #for file in filesToUpload:
        #    print(" - Loaded file", file)

        #print("exit")
        #exit(1)
        # Uploading files to the server
        storage = unicore_client.Storage(self.transport, self.env.urls["destiny_project_url"])
        print("Uploading...", self.env.urls["destiny_working_path"])
        for file in filesToUpload:
            storage.upload(os.path.join(self.env.urls["local_path"], file), destination=os.path.join(self.env.urls["destiny_working_path"], file))
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

    def run_trajectory(self, trajectory, iteration):
        print(iteration)
        return iteration

class Experiment_UNICORE(object):

    def __init__(self, environment, activate_log=True, **keyword_args):

        self.postprocessing = None
        self.run_id = 0

        #self.root_dir_path = environment["local_path"] #os.path.abspath(root_dir_path)
        self.logger = logging.getLogger('bin.l2l')
        #self.paths = None
        self.environment = environment
        self.trajectory = None
        self.optimizee = None
        self.optimizer = None

        self.logging = activate_log

        self.runner = None
        if 'runner' in keyword_args:
            if keyword_args['runner'] == "pyunicore":
                self.runner = PyUnicoreManager(environment=self.environment)

    def prepare_experiment(self, experiment_name, trajectory_name):
        #name = experiment_name
        if not os.path.isdir(self.environment.urls["local_path"]): #self.root_dir_path
            os.mkdir(self.environment.urls["local_path"]) #os.path.abspath(self.root_dir_path)
            print('Created a folder at {}'.format(self.environment.urls["local_path"])) #self.root_dir_path)

        self.paths = Paths(experiment_name, {},
                           root_dir_path=self.environment.urls["local_path"],
                           suffix="-" + trajectory_name)

        print("All output logs can be found in directory ",
              self.paths.logs_path)

        # Create an environment that handles running our simulation
        # Trajectory of each experiment
        self.environment.trajectory_info["trajectory_name"] = experiment_name
        self.environment.trajectory_info["filename"] = self.environment.urls["destiny_working_path"]  # self.paths.output_dir_path
        self.environment.trajectory_info["file_title"] = trajectory_name  # '{} data'.format(name)
        self.environment.trajectory_info["comment"] = "comments"  # '{} data'.format(name)
        self.environment.trajectory_info["add_time"] = True
        self.environment.trajectory_info["automatic_storing"] = True
        self.environment.trajectory_info["log_stdout"] = True  # kwargs.get('log_stdout', False),  # Sends stdout to logs
        #self.environment.trajectory_info["multiprocessing"] = True  # kwargs.get('multiprocessing', True)
        self.environment.createTrayectory()

        create_shared_logger_data(
            logger_names=['bin', 'optimizers'],
            log_levels=['INFO', 'INFO'],
            log_to_consoles=[True, True],
            sim_name='L2L-run',
            log_directory=self.paths.logs_path)
        configure_loggers()

        # Get the trajectory from the environment
        self.trajectory = self.environment.trajectory

        # Set trajectory
        default_pyunicore_params = dict()
        default_pyunicore_params["exec"] = "python3 " + os.path.join(self.paths.simulation_path, "run_files/run_optimizee.py")
        default_pyunicore_params["ready_file"] = os.path.join(self.paths.root_dir_path, "ready_files/ready_w_")
        default_pyunicore_params["work_path"] = self.paths.root_dir_path
        default_pyunicore_params["paths_obj"] = self.paths

        # Will contain all jube parameters
        all_jube_params = {}
        self.trajectory.f_add_parameter_group("PyUnicore_params",
                                        "Contains PyUnicore parameters")
        for k, v in default_pyunicore_params.items():
            self.trajectory.f_add_parameter_to_group("PyUnicore_params", k, v)

        return self.trajectory


    def prepare_optimizee(self, optimizee, path):
        # Serialize optimizee object so each process can run simulate on it independently on the CNs
        fname = os.path.join(path, "optimizee.bin")
        f = open(fname, "wb")
        pickle.dump(optimizee, f)
        f.close()
        logger.info("Serialized optimizee writen to path: {}".format(fname))

    def add_postprocessing(self, func):
        self.postprocessing = func

    def run_experiment(self, optimizer, optimizee, optimizer_parameters=None, optimizee_parameters=None):
        self.optimizee = optimizee
        self.optimizer = optimizer
        self.logger.info("Optimizee parameters: %s", optimizee_parameters)
        self.logger.info("Optimizer parameters: %s", optimizer_parameters)

        self.prepare_optimizee(optimizee, self.paths.simulation_path)

        # Add post processing
        self.add_postprocessing(optimizer.post_process)

        # Run the simulation
        self.run(optimizee.simulate)

    def end_experiment(self, optimizer):
        # Outer-loop optimizer end
        optimizer.end(self.trajectory)
        return self.trajectory, self.paths

    def run(self, runfunc):

        result = {}
        for it in range(self.trajectory.par['n_iteration']):

            if self.runner:
                logging.info("Environment run starting PyUnicore for n iterations: " + str(self.trajectory.par['n_iteration']))
                #jube = JUBERunner(self.trajectory)
                result[it] = []
                # Initialize new JUBE run and execute it
                try:
                    #jube.write_pop_for_jube(self.trajectory,it)
                    #result[it] = jube.run(self.trajectory,it)
                    # TODO: execute it!
                    result[it] = self.runner.run_trajectory(trajectory=self.trajectory, iteration=it)
                except Exception as e:
                    if self.logging:
                        logger.exception("Error launching PyUnicore run: " + str(e.__cause__))
                    raise e

            else:
                # Sequential calls to the runfunc in the optimizee
                result[it] = []
                # Call runfunc on each individual from the trajectory
                try:
                    for ind in self.trajectory.individuals[it]:
                        self.trajectory.individual = ind
                        result[it].append((ind.ind_idx, runfunc(self.trajectory)))
                        self.run_id = self.run_id + 1
                except Exception as e:
                    if self.logging:
                        logger.exception("Error during serial execution of individuals")
                        print("Error during serial execution of individuals")
                        raise
            # Add results to the trajectory
            self.trajectory.results.f_add_result_to_group("all_results", it, result[it])
            self.trajectory.current_results = result[it]
            # Perform the postprocessing step in order to generate the new parameter set
            self.postprocessing(self.trajectory, result[it])

        return result

################
"""
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