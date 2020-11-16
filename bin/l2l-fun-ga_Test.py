import numpy as np
from l2l.optimizees.functions import BenchmarkedFunctions, FunctionGeneratorOptimizee
from l2l.optimizers.evolution import GeneticAlgorithmParameters, GeneticAlgorithmOptimizer
from l2l.optimizees.functions import tools as function_tools
from l2l.utils.PyUnicoreManager import Environment_UNICORE, Experiment_UNICORE, PyUnicoreManager


from collections import namedtuple

#try:
#APM: modifications
# Prepare environment and experiment
environment = Environment_UNICORE(token="cGVyZXptYXJ0aW4xOmFwbUAzMDQwNTA",
                                  serverToConnect="https://zam2125.zam.kfa-juelich.de:9112/JUSUF/rest/core/",
                                  local_path='../results',
                                  destiny_working_path="perezmartin1/l2l_pyunicore",
                                  destiny_scriptname="Seeghorseshoe2",
                                  destiny_user_account="perezmartin1",
                                  destiny_project_path="/p/project/cslns",
                                  destiny_project_url="https://zam2125.zam.kfa-juelich.de:9112/JUSUF/rest/core/storages/PROJECT/",
                                  destiny_libraries_path="/p/project/cslns/perezmartin1/cmdstan/",
                                  script_language="python",
                                  script_name="",
                                  script_parameters="",
                                  needcompiler=False)

experiment = Experiment_UNICORE(environment=environment) #, runner="pyunicore"
traj = experiment.prepare_experiment(experiment_name='L2L-FUN-GA', trajectory_name="L2L-FUN-GA_Test")

# APM: modifications end

## Benchmark function
function_id = 4
bench_functs = BenchmarkedFunctions()
(benchmark_name, benchmark_function), benchmark_parameters = \
    bench_functs.get_function_by_index(function_id, noise=True)

optimizee_seed = 100
random_state = np.random.RandomState(seed=optimizee_seed)
function_tools.plot(benchmark_function, random_state)

## Innerloop simulator
optimizee = FunctionGeneratorOptimizee(traj, benchmark_function, seed=optimizee_seed)

## Outerloop optimizer initialization
parameters = GeneticAlgorithmParameters(seed=0, popsize=5, CXPB=0.5,
                                        MUTPB=0.3, NGEN=100, indpb=0.02,
                                        tournsize=15, matepar=0.5,
                                        mutpar=1
                                        )

optimizer = GeneticAlgorithmOptimizer(traj, optimizee_create_individual=optimizee.create_individual,
                                      optimizee_fitness_weights=(-0.1,),
                                      parameters=parameters)
experiment.run_experiment(optimizer=optimizer, optimizee=optimizee,
                          optimizee_parameters=parameters)
experiment.end_experiment(optimizer)

#except Exception as e:
#    print(e)