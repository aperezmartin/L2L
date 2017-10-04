from collections import namedtuple

import numpy as np
from sklearn.datasets import load_digits, fetch_mldata

from ltl.logging_tools import configure_loggers
from ltl.optimizees.optimizee import Optimizee
from .nn import NeuralNetworkClassifier

MNISTOptimizeeParameters = namedtuple('MNISTOptimizeeParameters', ['n_hidden', 'seed', 'use_small_mnist'])


class MNISTOptimizee(Optimizee):
    """
    Implements a simple function optimizee. Functions are generated using the FunctionGenerator.
    NOTE: Make sure the optimizee_fitness_weights is set to (-1,) to minimize the value of the function

    :param traj: The trajectory used to conduct the optimization.
    :param .MNISTOptimizeeParameters parameters:
    """

    def __init__(self, traj, parameters):
        super().__init__(traj)

        if parameters.use_small_mnist:
            # 8 x 8 images
            mnist_digits = load_digits()
            n_input = np.prod(mnist_digits.images.shape[1:])
            n_images = len(mnist_digits.images)  # 1797
            data_images = mnist_digits.images.reshape(n_images, -1) / 16.  # -> 1797 x 64
            data_targets = mnist_digits.target
        else:
            # 28 x 28 images
            mnist_digits = fetch_mldata('MNIST original')
            n_input = np.prod(mnist_digits.data.shape[1:])
            data_images = mnist_digits.data / 255.  # -> 70000 x 284
            data_targets = mnist_digits.target
        self.data_images, self.data_targets = data_images, data_targets

        seed = parameters.seed
        n_hidden = parameters.n_hidden
        self.recorder_parameters = parameters._asdict()

        seed = np.uint32(seed)
        self.random_state = np.random.RandomState(seed=seed)

        n_output = 10  # This is always true for mnist
        self.nn = NeuralNetworkClassifier(n_input, n_hidden, n_output)

        self.random_state = np.random.RandomState(seed=seed)

        # create_individual can be called because __init__ is complete except for traj initializtion
        indiv_dict = self.create_individual()
        for key, val in indiv_dict.items():
            traj.individual.f_add_parameter(key, val)
        traj.individual.f_add_parameter('seed', seed)

    def get_params(self):
        """
        Get the important parameters of the optimizee. This is used by :class:`ltl.recorder`
        for recording the optimizee parameters.

        :return: a :class:`dict`
        """
        return self.recorder_parameters

    def create_individual(self):
        """
        Creates a random value of parameter within given bounds
        """

        weight_shapes = self.nn.get_weights_shapes()
        total_num_weights = np.sum([np.product(weight_shape) for weight_shape in weight_shapes])

        return dict(weights=self.random_state.randn(total_num_weights))

    def bounding_func(self, individual):
        """
        Bounds the individual within the required bounds via coordinate clipping
        """
        return individual

    def simulate(self, traj):
        """
        Returns the value of the function chosen during initialization

        :param ~pypet.trajectory.Trajectory traj: Trajectory
        :return: a single element :obj:`tuple` containing the value of the chosen function
        """
        configure_loggers(exactly_once=True)  # logger configuration is here since this function is paralellised

        flattened_weights = traj.individual.weights
        weight_shapes = self.nn.get_weights_shapes()

        cumulative_num_weights_per_layer = np.cumsum([np.prod(weight_shape) for weight_shape in weight_shapes])

        weights = []
        for i, weight_shape in enumerate(weight_shapes):
            if i == 0:
                w = flattened_weights[:cumulative_num_weights_per_layer[i]].reshape(weight_shape)
            else:
                w = flattened_weights[
                    cumulative_num_weights_per_layer[i - 1]:cumulative_num_weights_per_layer[i]].reshape(weight_shape)
            weights.append(w)

        self.nn.set_weights(*weights)
        return self.nn.score(self.data_images, self.data_targets)