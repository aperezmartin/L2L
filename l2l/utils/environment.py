import time

from l2l.utils.trajectory import Trajectory
from l2l.utils.JUBE_runner import JUBERunner
import logging

logger = logging.getLogger("utils.Environment")


class Environment:
    """
    The Environment class takes the place of the pypet Environment and provides the required functionality
    to execute the inner loop. This means it uses either JUBE or sequential calls in order to execute all
    individuals in a generation.
    Based on the pypet environment concept: https://github.com/SmokinCaterpillar/pypet
    """

    def __init__(self, *args, **keyword_args):
        """
        Initializes an Environment
        :param args: arguments passed to the environment initialization
        :param keyword_args: arguments by keyword. Relevant keywords are trajectory and filename.
        The trajectory object holds individual parameters and history per generation of the exploration process.
        """
        if 'trajectory' in keyword_args:
            self.trajectory = Trajectory(name=keyword_args['trajectory'])
        if 'filename' in keyword_args:
            self.filename = keyword_args['filename']
        self.postprocessing = None
        self.multiprocessing = False#We don't use Jube
        if 'multiprocessing' in keyword_args:
            self.multiprocessing = keyword_args['multiprocessing']
        self.run_id = 0
        self.enable_logging()

    def run(self, runfunc):
        """
        Runs the optimizees using either JUBE or sequential calls.
        :param runfunc: The function to be called from the optimizee
        :return: the results of running a whole generation. Dictionary indexed by generation id.
        """
        result = {}
        start_outer = time.time()
        for it in range(self.trajectory.par['n_iteration']):
            start_it = time.time()
            print("---multiprocessing---", self.multiprocessing)
            if self.multiprocessing:
                # Multiprocessing is done through JUBE, either with or without scheduler
                logging.info("Environment run starting JUBERunner for n iterations: " + str(self.trajectory.par['n_iteration']))
                start = time.time()
                jube = JUBERunner(self.trajectory)
                result[it] = []
                # Initialize new JUBE run and execute it
                try:
                    jube.write_pop_for_jube(self.trajectory,it)
                    result[it] = jube.run(self.trajectory,it)
                except Exception as e:
                    if self.logging:
                        logger.exception("Error launching JUBE run: " + str(e.__cause__))
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
                except:
                    if self.logging:
                        logger.exception("Error during serial execution of individuals")
                    raise
            print("- optimizee simulation:", it, ", in ", round(time.time() - start_it, 6), "segs")
            # Add results to the trajectory
            start_postProc = time.time()
            self.trajectory.results.f_add_result_to_group("all_results", it, result[it])
            self.trajectory.current_results = result[it]
            # Perform the postprocessing step in order to generate the new parameter set
            self.postprocessing(self.trajectory, result[it])
            print("- postprocessing:",it ,", in ",round(time.time() - start_postProc, 6), "segs")
            print("")
        print("- Outerloop: in ", round(time.time() - start_outer, 6), "segs")

        return result

    def add_postprocessing(self, func):
        """
        Function to add a postprocessing step
        :param func: the function which performs the postprocessing. Postprocessing is the step where the results
        are assessed in order to produce a new set of parameters for the next generation.
        """
        self.postprocessing = func

    def enable_logging(self):
        """
        Function to enable logging
        TODO think about removing this.
        """
        self.logging = True

    def disable_logging(self):
        """
        Function to enable logging
        """
        self.logging = False
