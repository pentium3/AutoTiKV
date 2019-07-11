import gc
import numpy as np
import tensorflow as tf
import json
import logging
import string
from collections import OrderedDict
from random import choice
import random
import queue
import numpy as np
from sklearn.preprocessing import StandardScaler
from abc import ABCMeta, abstractmethod

from controller import knob_set, MEM_MAX

# -----------------PIPELINE CONSTANTS-----------------
#  the number of samples (staring points) in gradient descent
NUM_SAMPLES = 30
#  the number of selected tuning knobs
IMPORTANT_KNOB_NUMBER = 10
#  top K config with best performance put into prediction
TOP_NUM_CONFIG = 10
#  Initial probability to flip categorical feature in apply_constraints
#  server/analysis/constraints.py
INIT_FLIP_PROB = 0.3
#  The probability that we flip the i_th categorical feature is
#  FLIP_PROB_DECAY * (probability we flip (i-1)_th categorical feature)
FLIP_PROB_DECAY = 0.5

# -----------------GPR CONSTANTS-----------------
DEFAULT_LENGTH_SCALE = 1.0
DEFAULT_MAGNITUDE = 1.0
#  Max training size in GPR model
MAX_TRAIN_SIZE = 7000
#  Batch size in GPR model
BATCH_SIZE = 3000
# Threads for TensorFlow config
NUM_THREADS = 4

# -----------------GRADIENT DESCENT CONSTANTS-----------------
#  the maximum iterations of gradient descent
MAX_ITER = 500
#  a small bias when using training data points as starting points.
GPR_EPS = 0.001

DEFAULT_RIDGE = 0.01
DEFAULT_LEARNING_RATE = 0.01
DEFAULT_EPSILON = 1e-6
DEFAULT_SIGMA_MULTIPLIER = 3.0
DEFAULT_MU_MULTIPLIER = 1.0




# ==========================================================
#  Preprocessing Base Class
# ==========================================================
class Preprocess(object, metaclass=ABCMeta):

    @abstractmethod
    def fit(self, matrix):
        pass

    @abstractmethod
    def transform(self, matrix, copy=True):
        pass

    def fit_transform(self, matrix, copy=True):
        self.fit(matrix)
        return self.transform(matrix, copy=True)

    @abstractmethod
    def inverse_transform(self, matrix, copy=True):
        pass



class GPR(object):

    def __init__(self, length_scale=1.0, magnitude=1.0, max_train_size=7000,
                 batch_size=3000, num_threads=4, check_numerics=True, debug=False):
        assert np.isscalar(length_scale)
        assert np.isscalar(magnitude)
        assert length_scale > 0 and magnitude > 0
        self.length_scale = length_scale
        self.magnitude = magnitude
        self.max_train_size_ = max_train_size
        self.batch_size_ = batch_size
        self.num_threads_ = num_threads
        self.check_numerics = check_numerics
        self.debug = debug
        self.X_train = None
        self.y_train = None
        self.xy_ = None
        self.K = None
        self.K_inv = None
        self.graph = None
        self.vars = None
        self.ops = None

    def build_graph(self):
        self.vars = {}
        self.ops = {}
        self.graph = tf.Graph()
        with self.graph.as_default():
            mag_const = tf.constant(self.magnitude,
                                    dtype=np.float32,
                                    name='magnitude')
            ls_const = tf.constant(self.length_scale,
                                   dtype=np.float32,
                                   name='length_scale')

            # Nodes for distance computation
            v1 = tf.placeholder(tf.float32, name="v1")
            v2 = tf.placeholder(tf.float32, name="v2")
            dist_op = tf.sqrt(tf.reduce_sum(tf.pow(tf.subtract(v1, v2), 2), 1), name='dist_op')
            if self.check_numerics:
                dist_op = tf.check_numerics(dist_op, "dist_op: ")

            self.vars['v1_h'] = v1
            self.vars['v2_h'] = v2
            self.ops['dist_op'] = dist_op

            # Nodes for kernel computation
            X_dists = tf.placeholder(tf.float32, name='X_dists')
            ridge_ph = tf.placeholder(tf.float32, name='ridge')
            K_op = mag_const * tf.exp(-X_dists / ls_const)
            if self.check_numerics:
                K_op = tf.check_numerics(K_op, "K_op: ")
            K_ridge_op = K_op + tf.diag(ridge_ph)
            if self.check_numerics:
                K_ridge_op = tf.check_numerics(K_ridge_op, "K_ridge_op: ")

            self.vars['X_dists_h'] = X_dists
            self.vars['ridge_h'] = ridge_ph
            self.ops['K_op'] = K_op
            self.ops['K_ridge_op'] = K_ridge_op

            # Nodes for xy computation
            K = tf.placeholder(tf.float32, name='K')
            K_inv = tf.placeholder(tf.float32, name='K_inv')
            xy_ = tf.placeholder(tf.float32, name='xy_')
            yt_ = tf.placeholder(tf.float32, name='yt_')
            K_inv_op = tf.matrix_inverse(K)
            if self.check_numerics:
                K_inv_op = tf.check_numerics(K_inv_op, "K_inv: ")
            xy_op = tf.matmul(K_inv, yt_)
            if self.check_numerics:
                xy_op = tf.check_numerics(xy_op, "xy_: ")

            self.vars['K_h'] = K
            self.vars['K_inv_h'] = K_inv
            self.vars['xy_h'] = xy_
            self.vars['yt_h'] = yt_
            self.ops['K_inv_op'] = K_inv_op
            self.ops['xy_op'] = xy_op

            # Nodes for yhat/sigma computation
            K2 = tf.placeholder(tf.float32, name="K2")
            K3 = tf.placeholder(tf.float32, name="K3")
            yhat_ = tf.cast(tf.matmul(tf.transpose(K2), xy_), tf.float32)
            if self.check_numerics:
                yhat_ = tf.check_numerics(yhat_, "yhat_: ")
            sv1 = tf.matmul(tf.transpose(K2), tf.matmul(K_inv, K2))
            if self.check_numerics:
                sv1 = tf.check_numerics(sv1, "sv1: ")
            sig_val = tf.cast((tf.sqrt(tf.diag_part(K3 - sv1))), tf.float32)
            if self.check_numerics:
                sig_val = tf.check_numerics(sig_val, "sig_val: ")

            self.vars['K2_h'] = K2
            self.vars['K3_h'] = K3
            self.ops['yhat_op'] = yhat_
            self.ops['sig_op'] = sig_val

            # Compute y_best (min y)
            y_best_op = tf.cast(tf.reduce_min(yt_, 0, True), tf.float32)
            if self.check_numerics:
                y_best_op = tf.check_numerics(y_best_op, "y_best_op: ")
            self.ops['y_best_op'] = y_best_op

            sigma = tf.placeholder(tf.float32, name='sigma')
            yhat = tf.placeholder(tf.float32, name='yhat')

            self.vars['sigma_h'] = sigma
            self.vars['yhat_h'] = yhat

    def __repr__(self):
        rep = ""
        for k, v in sorted(self.__dict__.items()):
            rep += "{} = {}\n".format(k, v)
        return rep

    def __str__(self):
        return self.__repr__()

    def check_X_y(self, X, y):
        from sklearn.utils.validation import check_X_y

        if X.shape[0] > self.max_train_size_:
            raise Exception("X_train size cannot exceed {} ({})"
                            .format(self.max_train_size_, X.shape[0]))
        return check_X_y(X, y, multi_output=True,
                         allow_nd=True, y_numeric=True,
                         estimator="GPR")

    def check_fitted(self):
        if self.X_train is None or self.y_train is None \
                or self.xy_ is None or self.K is None:
            raise Exception("The model must be trained before making predictions!")

    @staticmethod
    def check_array(X):
        from sklearn.utils.validation import check_array
        return check_array(X, allow_nd=True, estimator="GPR")

    @staticmethod
    def check_output(X):
        finite_els = np.isfinite(X)
        if not np.all(finite_els):
            raise Exception("Input contains non-finite values: {}"
                            .format(X[~finite_els]))

    def fit(self, X_train, y_train, ridge=1.0):
        self._reset()
        X_train, y_train = self.check_X_y(X_train, y_train)
        self.X_train = np.float32(X_train)
        self.y_train = np.float32(y_train)
        sample_size = self.X_train.shape[0]

        if np.isscalar(ridge):
            ridge = np.ones(sample_size) * ridge
        assert isinstance(ridge, np.ndarray)
        assert ridge.ndim == 1

        X_dists = np.zeros((sample_size, sample_size), dtype=np.float32)
        with tf.Session(graph=self.graph,
                        config=tf.ConfigProto(
                            intra_op_parallelism_threads=self.num_threads_)) as sess:
            dist_op = self.ops['dist_op']
            v1, v2 = self.vars['v1_h'], self.vars['v2_h']
            for i in range(sample_size):
                X_dists[i] = sess.run(dist_op, feed_dict={v1: self.X_train[i], v2: self.X_train})

            K_ridge_op = self.ops['K_ridge_op']
            X_dists_ph = self.vars['X_dists_h']
            ridge_ph = self.vars['ridge_h']

            self.K = sess.run(K_ridge_op, feed_dict={X_dists_ph: X_dists, ridge_ph: ridge})

            K_ph = self.vars['K_h']

            K_inv_op = self.ops['K_inv_op']
            self.K_inv = sess.run(K_inv_op, feed_dict={K_ph: self.K})

            xy_op = self.ops['xy_op']
            K_inv_ph = self.vars['K_inv_h']
            yt_ph = self.vars['yt_h']
            self.xy_ = sess.run(xy_op, feed_dict={K_inv_ph: self.K_inv,
                                                  yt_ph: self.y_train})
        return self

    def predict(self, X_test):
        self.check_fitted()
        X_test = np.float32(GPR.check_array(X_test))
        test_size = X_test.shape[0]
        sample_size = self.X_train.shape[0]

        arr_offset = 0
        yhats = np.zeros([test_size, 1])
        sigmas = np.zeros([test_size, 1])
        with tf.Session(graph=self.graph,
                        config=tf.ConfigProto(
                            intra_op_parallelism_threads=self.num_threads_)) as sess:
            # Nodes for distance operation
            dist_op = self.ops['dist_op']
            v1 = self.vars['v1_h']
            v2 = self.vars['v2_h']

            # Nodes for kernel computation
            K_op = self.ops['K_op']
            X_dists = self.vars['X_dists_h']

            # Nodes to compute yhats/sigmas
            yhat_ = self.ops['yhat_op']
            K_inv_ph = self.vars['K_inv_h']
            K2 = self.vars['K2_h']
            K3 = self.vars['K3_h']
            xy_ph = self.vars['xy_h']

            while arr_offset < test_size:
                if arr_offset + self.batch_size_ > test_size:
                    end_offset = test_size
                else:
                    end_offset = arr_offset + self.batch_size_

                X_test_batch = X_test[arr_offset:end_offset]
                batch_len = end_offset - arr_offset

                dists1 = np.zeros([sample_size, batch_len])
                for i in range(sample_size):
                    dists1[i] = sess.run(dist_op, feed_dict={v1: self.X_train[i],
                                                             v2: X_test_batch})

                sig_val = self.ops['sig_op']
                K2_ = sess.run(K_op, feed_dict={X_dists: dists1})
                yhat = sess.run(yhat_, feed_dict={K2: K2_, xy_ph: self.xy_})
                dists2 = np.zeros([batch_len, batch_len])
                for i in range(batch_len):
                    dists2[i] = sess.run(dist_op, feed_dict={v1: X_test_batch[i], v2: X_test_batch})
                K3_ = sess.run(K_op, feed_dict={X_dists: dists2})

                sigma = np.zeros([1, batch_len], np.float32)
                sigma[0] = sess.run(sig_val, feed_dict={K_inv_ph: self.K_inv, K2: K2_, K3: K3_})
                sigma = np.transpose(sigma)
                yhats[arr_offset: end_offset] = yhat
                sigmas[arr_offset: end_offset] = sigma
                arr_offset = end_offset
        GPR.check_output(yhats)
        GPR.check_output(sigmas)
        return GPRResult(yhats, sigmas)

    def get_params(self, deep=True):
        return {"length_scale": self.length_scale,
                "magnitude": self.magnitude,
                "X_train": self.X_train,
                "y_train": self.y_train,
                "xy_": self.xy_,
                "K": self.K,
                "K_inv": self.K_inv}

    def set_params(self, **parameters):
        for param, val in list(parameters.items()):
            setattr(self, param, val)
        return self

    def _reset(self):
        self.X_train = None
        self.y_train = None
        self.xy_ = None
        self.K = None
        self.K_inv = None
        self.graph = None
        self.build_graph()
        gc.collect()


class GPRResult(object):

    def __init__(self, ypreds=None, sigmas=None):
        self.ypreds = ypreds
        self.sigmas = sigmas


class GPRGD(GPR):

    GP_BETA_UCB = "UCB"
    GP_BETA_CONST = "CONST"

    def __init__(self,
                 length_scale=1.0,
                 magnitude=1.0,
                 max_train_size=7000,
                 batch_size=3000,
                 num_threads=4,
                 learning_rate=0.01,
                 epsilon=1e-6,
                 max_iter=100,
                 sigma_multiplier=3.0,
                 mu_multiplier=1.0):
        super(GPRGD, self).__init__(length_scale=length_scale,
                                    magnitude=magnitude,
                                    max_train_size=max_train_size,
                                    batch_size=batch_size,
                                    num_threads=num_threads)
        self.learning_rate = learning_rate
        self.epsilon = epsilon
        self.max_iter = max_iter
        self.sigma_multiplier = sigma_multiplier
        self.mu_multiplier = mu_multiplier
        self.X_min = None
        self.X_max = None

    def fit(self, X_train, y_train, X_min, X_max, ridge):  # pylint: disable=arguments-differ
        super(GPRGD, self).fit(X_train, y_train, ridge)
        self.X_min = X_min
        self.X_max = X_max

        with tf.Session(graph=self.graph,
                        config=tf.ConfigProto(
                            intra_op_parallelism_threads=self.num_threads_)) as sess:
            xt_ = tf.Variable(self.X_train[0], tf.float32)
            xt_ph = tf.placeholder(tf.float32)
            xt_assign_op = xt_.assign(xt_ph)
            init = tf.global_variables_initializer()
            sess.run(init)
            K2_mat = tf.transpose(tf.expand_dims(tf.sqrt(tf.reduce_sum(tf.pow(
                tf.subtract(xt_, self.X_train), 2), 1)), 0))
            if self.check_numerics is True:
                K2_mat = tf.check_numerics(K2_mat, "K2_mat: ")
            K2__ = tf.cast(self.magnitude * tf.exp(-K2_mat / self.length_scale), tf.float32)
            if self.check_numerics is True:
                K2__ = tf.check_numerics(K2__, "K2__: ")
            yhat_gd = tf.cast(tf.matmul(tf.transpose(K2__), self.xy_), tf.float32)
            if self.check_numerics is True:
                yhat_gd = tf.check_numerics(yhat_gd, message="yhat: ")
            sig_val = tf.cast((tf.sqrt(self.magnitude - tf.matmul(
                tf.transpose(K2__), tf.matmul(self.K_inv, K2__)))), tf.float32)
            if self.check_numerics is True:
                sig_val = tf.check_numerics(sig_val, message="sigma: ")
            # LOG.debug("\nyhat_gd : %s", str(sess.run(yhat_gd)))
            # LOG.debug("\nsig_val : %s", str(sess.run(sig_val)))

            loss = tf.squeeze(tf.subtract(self.mu_multiplier * yhat_gd,
                                          self.sigma_multiplier * sig_val))
            if self.check_numerics is True:
                loss = tf.check_numerics(loss, "loss: ")
            optimizer = tf.train.AdamOptimizer(learning_rate=self.learning_rate,
                                               epsilon=self.epsilon)
            # optimizer = tf.train.GradientDescentOptimizer(learning_rate=self.learning_rate)
            train = optimizer.minimize(loss)

            self.vars['xt_'] = xt_
            self.vars['xt_ph'] = xt_ph
            self.ops['xt_assign_op'] = xt_assign_op
            self.ops['yhat_gd'] = yhat_gd
            self.ops['sig_val2'] = sig_val
            self.ops['loss_op'] = loss
            self.ops['train_op'] = train
        return self

    def predict(self, X_test, constraint_helper=None,  # pylint: disable=arguments-differ
                categorical_feature_method='hillclimbing',
                categorical_feature_steps=3):
        self.check_fitted()
        X_test = np.float32(GPR.check_array(X_test))
        test_size = X_test.shape[0]
        nfeats = self.X_train.shape[1]

        arr_offset = 0
        yhats = np.zeros([test_size, 1])
        sigmas = np.zeros([test_size, 1])
        minls = np.zeros([test_size, 1])
        minl_confs = np.zeros([test_size, nfeats])

        with tf.Session(graph=self.graph,
                        config=tf.ConfigProto(
                            intra_op_parallelism_threads=self.num_threads_)) as sess:
            while arr_offset < test_size:
                if arr_offset + self.batch_size_ > test_size:
                    end_offset = test_size
                else:
                    end_offset = arr_offset + self.batch_size_

                X_test_batch = X_test[arr_offset:end_offset]
                batch_len = end_offset - arr_offset

                xt_ = self.vars['xt_']
                init = tf.global_variables_initializer()
                sess.run(init)

                sig_val = self.ops['sig_val2']
                yhat_gd = self.ops['yhat_gd']
                loss = self.ops['loss_op']
                train = self.ops['train_op']

                xt_ph = self.vars['xt_ph']
                assign_op = self.ops['xt_assign_op']

                yhat = np.empty((batch_len, 1))
                sigma = np.empty((batch_len, 1))
                minl = np.empty((batch_len, 1))
                minl_conf = np.empty((batch_len, nfeats))
                for i in range(batch_len):
                    # if self.debug is True:
                    #     LOG.info("-------------------------------------------")
                    yhats_it = np.empty((self.max_iter + 1,)) * np.nan
                    sigmas_it = np.empty((self.max_iter + 1,)) * np.nan
                    losses_it = np.empty((self.max_iter + 1,)) * np.nan
                    confs_it = np.empty((self.max_iter + 1, nfeats)) * np.nan

                    sess.run(assign_op, feed_dict={xt_ph: X_test_batch[i]})
                    step = 0
                    for step in range(self.max_iter):
                        # if self.debug is True:
                        #     LOG.info("Batch %d, iter %d:", i, step)
                        yhats_it[step] = sess.run(yhat_gd)[0][0]
                        sigmas_it[step] = sess.run(sig_val)[0][0]
                        losses_it[step] = sess.run(loss)
                        confs_it[step] = sess.run(xt_)
                        # if self.debug is True:
                        #     LOG.info("    yhat:  %s", str(yhats_it[step]))
                        #     LOG.info("    sigma: %s", str(sigmas_it[step]))
                        #     LOG.info("    loss:  %s", str(losses_it[step]))
                        #     LOG.info("    conf:  %s", str(confs_it[step]))
                        sess.run(train)
                        # constraint Projected Gradient Descent
                        xt = sess.run(xt_)
                        xt_valid = np.minimum(xt, self.X_max)
                        xt_valid = np.maximum(xt_valid, self.X_min)
                        sess.run(assign_op, feed_dict={xt_ph: xt_valid})
                        if constraint_helper is not None:
                            xt_valid = constraint_helper.apply_constraints(sess.run(xt_))
                            sess.run(assign_op, feed_dict={xt_ph: xt_valid})
                            if categorical_feature_method == 'hillclimbing':
                                if step % categorical_feature_steps == 0:
                                    current_xt = sess.run(xt_)
                                    current_loss = sess.run(loss)
                                    new_xt = \
                                        constraint_helper.randomize_categorical_features(
                                            current_xt)
                                    sess.run(assign_op, feed_dict={xt_ph: new_xt})
                                    new_loss = sess.run(loss)
                                    if current_loss < new_loss:
                                        sess.run(assign_op, feed_dict={xt_ph: new_xt})
                            else:
                                raise Exception("Unknown categorial feature method: {}".format(
                                    categorical_feature_method))
                    if step == self.max_iter - 1:
                        # Record results from final iteration
                        yhats_it[-1] = sess.run(yhat_gd)[0][0]
                        sigmas_it[-1] = sess.run(sig_val)[0][0]
                        losses_it[-1] = sess.run(loss)
                        confs_it[-1] = sess.run(xt_)
                        assert np.all(np.isfinite(yhats_it))
                        assert np.all(np.isfinite(sigmas_it))
                        assert np.all(np.isfinite(losses_it))
                        assert np.all(np.isfinite(confs_it))

                    # Store info for conf with min loss from all iters
                    if np.all(~np.isfinite(losses_it)):
                        min_loss_idx = 0
                    else:
                        min_loss_idx = np.nanargmin(losses_it)
                    yhat[i] = yhats_it[min_loss_idx]
                    sigma[i] = sigmas_it[min_loss_idx]
                    minl[i] = losses_it[min_loss_idx]
                    minl_conf[i] = confs_it[min_loss_idx]

                minls[arr_offset:end_offset] = minl
                minl_confs[arr_offset:end_offset] = minl_conf
                yhats[arr_offset:end_offset] = yhat
                sigmas[arr_offset:end_offset] = sigma
                arr_offset = end_offset

        GPR.check_output(yhats)
        GPR.check_output(sigmas)
        GPR.check_output(minls)
        GPR.check_output(minl_confs)

        return GPRGDResult(yhats, sigmas, minls, minl_confs)

    @staticmethod
    def calculate_sigma_multiplier(t, ndim, bound=0.1):
        assert t > 0
        assert ndim > 0
        assert bound > 0 and bound <= 1
        beta = 2 * np.log(ndim * (t**2) * (np.pi**2) / 6 * bound)
        if beta > 0:
            beta = np.sqrt(beta)
        else:
            beta = 1
        return beta


class GPRGDResult(GPRResult):

    def __init__(self, ypreds=None, sigmas=None,
                 minl=None, minl_conf=None):
        super(GPRGDResult, self).__init__(ypreds, sigmas)
        self.minl = minl
        self.minl_conf = minl_conf


class ParamConstraintHelper(object):

    def __init__(self, scaler, encoder=None, binary_vars=None,
                 init_flip_prob=0.3, flip_prob_decay=0.5):
        if 'inverse_transform' not in dir(scaler):
            raise Exception("Scaler object must provide function inverse_transform(X)")
        if 'transform' not in dir(scaler):
            raise Exception("Scaler object must provide function transform(X)")
        self.scaler_ = scaler
        if encoder is not None and len(encoder.n_values) > 0:
            self.is_dummy_encoded_ = True
            self.encoder_ = encoder.encoder
        else:
            self.is_dummy_encoded_ = False
        self.binary_vars_ = binary_vars
        self.init_flip_prob_ = init_flip_prob
        self.flip_prob_decay_ = flip_prob_decay

    def apply_constraints(self, sample, scaled=True, rescale=True):
        conv_sample = self._handle_scaling(sample, scaled)

        if self.is_dummy_encoded_:
            # apply categorical (ie enum var, >=3 values) constraints
            n_values = self.encoder_.n_values_
            cat_start_indices = self.encoder_.feature_indices_
            for i, nvals in enumerate(n_values):
                start_idx = cat_start_indices[i]
                cvals = conv_sample[start_idx: start_idx + nvals]
                cvals = np.array(np.arange(nvals) == np.argmax(cvals), dtype=float)
                assert np.sum(cvals) == 1
                conv_sample[start_idx: start_idx + nvals] = cvals

        # apply binary (0-1) constraints
        if self.binary_vars_ is not None:
            for i in self.binary_vars_:
                # round to closest
                if conv_sample[i] >= 0.5:
                    conv_sample[i] = 1
                else:
                    conv_sample[i] = 0

        conv_sample = self._handle_rescaling(conv_sample, rescale)
        return conv_sample

    def _handle_scaling(self, sample, scaled):
        if scaled:
            if sample.ndim == 1:
                sample = sample.reshape(1, -1)
            sample = self.scaler_.inverse_transform(sample).ravel()
        else:
            sample = np.array(sample)
        return sample

    def _handle_rescaling(self, sample, rescale):
        if rescale:
            if sample.ndim == 1:
                sample = sample.reshape(1, -1)
            return self.scaler_.transform(sample).ravel()
        return sample

    def randomize_categorical_features(self, sample, scaled=True, rescale=True):
        # If there are no categorical features, this function is a no-op.
        if not self.is_dummy_encoded_:
            return sample
        n_values = self.encoder_.n_values_
        cat_start_indices = self.encoder_.feature_indices_
        n_cat_feats = len(n_values)

        conv_sample = self._handle_scaling(sample, scaled)
        flips = np.zeros((n_cat_feats,), dtype=bool)

        # Always flip at least one categorical feature
        flips[0] = True

        # Flip the rest with decreasing probability
        p = self.init_flip_prob_
        for i in range(1, n_cat_feats):
            if np.random.rand() <= p:
                flips[i] = True
            p *= self.flip_prob_decay_

        flip_shuffle_indices = np.random.choice(np.arange(n_cat_feats),
                                                n_cat_feats,
                                                replace=False)
        flips = flips[flip_shuffle_indices]

        for i, nvals in enumerate(n_values):
            if flips[i]:
                start_idx = cat_start_indices[i]
                current_val = conv_sample[start_idx: start_idx + nvals]
                assert np.all(np.logical_or(current_val == 0, current_val == 1)), \
                    "categorical {0}: value not 0/1: {1}".format(i, current_val)
                choices = np.arange(nvals)[current_val != 1]
                assert choices.size == nvals - 1
                r = np.zeros(nvals)
                r[np.random.choice(choices)] = 1
                assert np.sum(r) == 1
                conv_sample[start_idx: start_idx + nvals] = r

        conv_sample = self._handle_rescaling(conv_sample, rescale)
        return conv_sample


class DummyEncoder(Preprocess):

    def __init__(self, n_values, categorical_features, cat_columnlabels, noncat_columnlabels):
        from sklearn.preprocessing import OneHotEncoder

        if not isinstance(n_values, np.ndarray):
            n_values = np.array(n_values)
        if not isinstance(categorical_features, np.ndarray):
            categorical_features = np.array(categorical_features)
        # assert categorical_features.size > 0
        assert categorical_features.shape == n_values.shape
        for nv in n_values:
            if nv <= 2:
                raise Exception("Categorical features must have 3+ labels")

        self.n_values = n_values
        self.cat_columnlabels = cat_columnlabels
        self.noncat_columnlabels = noncat_columnlabels
        self.encoder = OneHotEncoder(
            n_values=n_values, categorical_features=categorical_features, sparse=False)
        self.new_labels = None
        self.cat_idxs_old = categorical_features

    def fit(self, matrix):
        self.encoder.fit(matrix)
        # determine new columnlabels
        # categorical variables are done in order specified by categorical_features
        new_labels = []
        for i, cat_label in enumerate(self.cat_columnlabels):
            low = self.encoder.feature_indices_[i]
            high = self.encoder.feature_indices_[i + 1]
            for j in range(low, high):
                # eg the categorical variable named cat_var with 5 possible values
                # turns into 0/1 variables named cat_var____0, ..., cat_var____4
                new_labels.append(cat_label + "____" + str(j - low))
        # according to sklearn documentation,
        # "non-categorical features are always stacked to the right of the matrix"
        # by observation, it looks like the non-categorical features' relative order is preserved
        # BUT: there is no guarantee made about that behavior!
        # We either trust OneHotEncoder to be sensible, or look for some other way
        new_labels += self.noncat_columnlabels
        self.new_labels = new_labels

    def transform(self, matrix, copy=True):
        # actually transform the matrix
        matrix_encoded = self.encoder.transform(matrix)
        return matrix_encoded

    def fit_transform(self, matrix, copy=True):
        self.fit(matrix)
        return self.transform(matrix)

    def inverse_transform(self, matrix, copy=True):
        n_values = self.n_values
        # If there are no categorical variables, no transformation happened.
        if len(n_values) == 0:
            return matrix

        # Otherwise, this is a dummy-encoded matrix. Transform it back to original form.
        n_features = matrix.shape[-1] - self.encoder.feature_indices_[-1] + len(n_values)
        noncat_start_idx = self.encoder.feature_indices_[-1]
        inverted_matrix = np.empty((matrix.shape[0], n_features))
        cat_idx = 0
        noncat_idx = 0
        for i in range(n_features):
            if i in self.cat_idxs_old:
                new_col = np.ones((matrix.shape[0],))
                start_idx = self.encoder.feature_indices_[cat_idx]
                for j in range(n_values[cat_idx]):
                    col = matrix[:, start_idx + j]
                    new_col[col == 1] = j
                cat_idx += 1
            else:
                new_col = np.array(matrix[:, noncat_start_idx + noncat_idx])
                noncat_idx += 1
            inverted_matrix[:, i] = new_col
        return inverted_matrix

    def total_dummies(self):
        return sum(self.n_values)


def combine_duplicate_rows(X_matrix, y_matrix, rowlabels):
    X_unique, idxs, invs, cts = np.unique(X_matrix,
                                          return_index=True,
                                          return_inverse=True,
                                          return_counts=True,
                                          axis=0)
    num_unique = X_unique.shape[0]
    if num_unique == X_matrix.shape[0]:
        # No duplicate rows

        # For consistency, tuple the rowlabels
        rowlabels = np.array([tuple([x]) for x in rowlabels])  # pylint: disable=bad-builtin,deprecated-lambda
        return X_matrix, y_matrix, rowlabels

    # Combine duplicate rows
    y_unique = np.empty((num_unique, y_matrix.shape[1]))
    rowlabels_unique = np.empty(num_unique, dtype=tuple)
    ix = np.arange(X_matrix.shape[0])
    #print(ix, invs)
    for i, count in enumerate(cts):
        #print(i, cts)
        if count == 1:
            y_unique[i, :] = y_matrix[idxs[i], :]
            rowlabels_unique[i] = (rowlabels[idxs[i]],)
        else:
            dup_idxs = ix[invs == i]
            y_unique[i, :] = np.median(y_matrix[dup_idxs, :], axis=0)
            rowlabels_unique[i] = tuple(rowlabels[dup_idxs])
    return X_unique, y_unique, rowlabels_unique


def dummy_encoder_helper(featured_knobs):
    n_values = []
    cat_knob_indices = []
    cat_knob_names = []
    noncat_knob_names = []
    binary_knob_indices = []
    #dbms_info = DBMSCatalog.objects.filter(pk=dbms.pk)
    #if len(dbms_info) == 0:
    #    raise Exception("DBMSCatalog cannot find dbms {}".format(dbms.full_name()))
    #full_dbms_name = dbms_info[0]

    for i, knob_name in enumerate(featured_knobs):
        # knob can be uniquely identified by (dbms, knob_name)
        #knobs = KnobCatalog.objects.filter(name=knob_name, dbms=dbms)
        # __INPUT__ all knobs of current dbms
        #if len(knobs) == 0:
        #    raise Exception("KnobCatalog cannot find knob of name {} in {}".format(knob_name, full_dbms_name))
        #knob = knobs[0]
        # __INPUT__ type of knob value (from \ottertune\server\website\website\fixtures\postgres-96_knobs.json)
        knob = knob_set[knob_name]

        # check if knob is ENUM
        #if knob.vartype == VarType.ENUM:
        if knob['type'] == "enum":
            # enumvals is a comma delimited list
            #enumvals = knob.enumvals.split(",")
            enumvals = knob['enumval']
            if len(enumvals) > 2:
                # more than 2 values requires dummy encoding
                n_values.append(len(enumvals))
                cat_knob_indices.append(i)
                cat_knob_names.append(knob_name)
            else:
                # knob is binary
                noncat_knob_names.append(knob_name)
                binary_knob_indices.append(i)
        else:
            #if knob.vartype == VarType.BOOL:
            if knob['type'] == 'bool':
                binary_knob_indices.append(i)
            noncat_knob_names.append(knob_name)

    n_values = np.array(n_values)
    cat_knob_indices = np.array(cat_knob_indices)
    categorical_info = {'n_values': n_values,
                        'categorical_features': cat_knob_indices,
                        'cat_columnlabels': cat_knob_names,
                        'noncat_columnlabels': noncat_knob_names,
                        'binary_vars': binary_knob_indices}
    return categorical_info


def gen_random_data(target_data):
    random_knob_result = {}
    for name in target_data.knob_labels:
        vartype = knob_set[name]['type']
        if vartype == 'bool':
            flag = random.randint(0, 1)
            if flag == 0:
                random_knob_result[name] = False
            else:
                random_knob_result[name] = True
        elif vartype == 'enum':
            enumvals = knob_set[name]['enumval']
            enumvals_len = len(enumvals)
            rand_idx = random.randint(0, enumvals_len - 1)
            random_knob_result[name] = rand_idx
        elif vartype == 'int':
            minval=knob_set[name]['minval']
            maxval=knob_set[name]['maxval']
            random_knob_result[name] = random.randint(int(minval), int(maxval))
        elif vartype == 'real':
            minval=knob_set[name]['minval']
            maxval=knob_set[name]['maxval']
            random_knob_result[name] = random.uniform(float(minval), float(maxval))
        # elif vartype == STRING:
        #     random_knob_result[name] = "None"
        # elif vartype == TIMESTAMP:
        #     random_knob_result[name] = "None"
    return random_knob_result


def configuration_recommendation(target_data):
    if(target_data.num_previousamples<10):                               # TODO: Could give random recommendation on several rounds at first, rather than only the first one round.
        return gen_random_data(target_data)
    #target_data['X_matrix'] = previous_knob_set                         #__INPUT__ (num of samples*num of knobs)
    #target_data['y_matrix'] = previous_metric_set                       #__INPUT__ (num of samples*num of metrics)
    #workload_knob_data = mapped_workload_knob_dataset                   #__INPUT__
    #workload_metric_data = mapped_workload_metric_dataset               #__INPUT__

    #X_workload = np.array(workload_knob_data['data'])                    #(num of mapped workloads*num of knobs)
    X_workload = target_data.new_knob_set
    #X_columnlabels = np.array(workload_knob_data['columnlabels'])        #name of knobs
    X_columnlabels = target_data.knob_labels
    #y_workload = np.array(workload_metric_data['data'])                  #(num of mapped workloads*num of knobs)
    y_workload = target_data.new_metric_set
    #y_columnlabels = np.array(workload_metric_data['columnlabels'])      #name of metrics
    y_columnlabels = target_data.metric_labels
    #rowlabels_workload = np.array(workload_metric_data['rowlabels'])
    rowlabels_workload = target_data.new_rowlabels

    # Target workload data
    #newest_result = Result.objects.get(pk=target_data['newest_result_id'])  #__INPUT__ info about target(newest) workload
    #    newest_result.session.dbms = Postgres v9.6
    #    newest_result.workload.hardware.memory = memory size of current PC (GB) (Default=15.0, from 'm3.xlarge' configuration)

    #X_target = target_data['X_matrix']
    X_target = target_data.previous_knob_set
    #y_target = target_data['y_matrix']
    y_target = target_data.previous_metric_set
    #rowlabels_target = np.array(target_data['rowlabels'])
    rowlabels_target = target_data.previous_rowlabels

    # Filter Xs by top 10 ranked knobs
    #ranked_knobs = important_knobs              #__INPUT__ name of top IMPORTANT_KNOB_NUMBER important knobs
    #ranked_knob_idxs = important_knobs          #__INPUT__ idx of top IMPORTANT_KNOB_NUMBER important knobs
    #X_workload = X_workload[:, ranked_knob_idxs]
    #X_target = X_target[:, ranked_knob_idxs]
    #X_columnlabels = X_columnlabels[ranked_knob_idxs]

    # Filter ys by current target objective metric
    #target_objective = name_of_target_metric                        #__INPUT__ only 1 target metric to be optimized
    target_objective = target_data.target_metric
    target_obj_idx = [i for i, cl in enumerate(y_columnlabels) if cl == target_objective]   #idx of target metric in y_columnlabels matrix

    #lessisbetter = False            #__INPUT__ True: lower metric value is better(like latency).    False: higher metric value is better(like throughput).
    lessisbetter = target_data.target_lessisbetter==1

    y_workload = y_workload[:, target_obj_idx]
    y_target = y_target[:, target_obj_idx]
    y_columnlabels = y_columnlabels[target_obj_idx]

    # Combine duplicate rows in the target/workload data (separately)
    X_workload, y_workload, rowlabels_workload = combine_duplicate_rows(X_workload, y_workload, rowlabels_workload)
    X_target, y_target, rowlabels_target = combine_duplicate_rows(X_target, y_target, rowlabels_target)

    # Delete any rows that appear in both the workload data and the target
    # data from the workload data
    dups_filter = np.ones(X_workload.shape[0], dtype=bool)
    target_row_tups = [tuple(row) for row in X_target]
    for i, row in enumerate(X_workload):
        if tuple(row) in target_row_tups:
            dups_filter[i] = False
    X_workload = X_workload[dups_filter, :]
    y_workload = y_workload[dups_filter, :]
    rowlabels_workload = rowlabels_workload[dups_filter]

    # Combine target & workload Xs for preprocessing
    X_matrix = np.vstack([X_target, X_workload])

    # Dummy encode categorial variables
    categorical_info = dummy_encoder_helper(X_columnlabels)      #__INPUT__
        #    mapped_workload.dbms = Postgres v9.6
    dummy_encoder = DummyEncoder(categorical_info['n_values'], categorical_info['categorical_features'], categorical_info['cat_columnlabels'], categorical_info['noncat_columnlabels'])
    X_matrix = dummy_encoder.fit_transform(X_matrix)

    # below two variables are needed for correctly determing max/min on dummies
    binary_index_set = set(categorical_info['binary_vars'])
    total_dummies = dummy_encoder.total_dummies()

    # Scale to N(0, 1)
    X_scaler = StandardScaler()
    X_scaled = X_scaler.fit_transform(X_matrix)
    if y_target.shape[0] < 5:  # FIXME
        # FIXME (dva): if there are fewer than 5 target results so far
        # then scale the y values (metrics) using the workload's
        # y_scaler. I'm not sure if 5 is the right cutoff.
        y_target_scaler = None
        y_workload_scaler = StandardScaler()
        y_matrix = np.vstack([y_target, y_workload])
        y_scaled = y_workload_scaler.fit_transform(y_matrix)
    else:
        # FIXME (dva): otherwise try to compute a separate y_scaler for
        # the target and scale them separately.
        try:
            y_target_scaler = StandardScaler()
            y_workload_scaler = StandardScaler()
            y_target_scaled = y_target_scaler.fit_transform(y_target)
            y_workload_scaled = y_workload_scaler.fit_transform(y_workload)
            y_scaled = np.vstack([y_target_scaled, y_workload_scaled])
        except ValueError:
            y_target_scaler = None
            y_workload_scaler = StandardScaler()
            y_scaled = y_workload_scaler.fit_transform(y_target)

    # Set up constraint helper
    constraint_helper = ParamConstraintHelper(scaler=X_scaler, 
                                              encoder=dummy_encoder, 
                                              binary_vars=categorical_info['binary_vars'], 
                                              init_flip_prob=INIT_FLIP_PROB, 
                                              flip_prob_decay=FLIP_PROB_DECAY)    #__INPUT__

    # FIXME (dva): check if these are good values for the ridge
    # ridge = np.empty(X_scaled.shape[0])
    # ridge[:X_target.shape[0]] = 0.01
    # ridge[X_target.shape[0]:] = 0.1

    # FIXME: we should generate more samples and use a smarter sampling
    # technique
    num_samples = NUM_SAMPLES
    X_samples = np.empty((num_samples, X_scaled.shape[1]))
    X_min = np.empty(X_scaled.shape[1])
    X_max = np.empty(X_scaled.shape[1])

    #knobs_mem = KnobCatalog.objects.filter(dbms=newest_result.session.dbms, tunable=True, resource=1)
    #knobs_mem_catalog = {k.name: k for k in knobs_mem}
    #    newest_result.session.dbms = Postgres v9.6
    #    __INPUT__ all knobs that are tunable and related to PC memory (from \ottertune\server\website\website\fixtures\postgres-96_knobs.json)

    #mem_max = newest_result.workload.hardware.memory
    #mem_max = MEM_MAX
    #    __INPUT__ newest_result.workload.hardware.memory = memory size of current PC (GB) (Default=15.0, from 'm3.xlarge' configuration)

    X_mem = np.zeros([1, X_scaled.shape[1]])
    X_default = np.empty(X_scaled.shape[1])

    # Get default knob values
    for i, k_name in enumerate(X_columnlabels):
        #k = KnobCatalog.objects.filter(dbms=newest_result.session.dbms, name=k_name)[0]
        #    newest_result.session.dbms = Postgres v9.6
        #X_default[i] = k.default
        #    __INPUT__ default value of these knobs (from \ottertune\server\website\website\fixtures\postgres-96_knobs.json)
        X_default[i] = knob_set[k_name]['default']

    X_default_scaled = X_scaler.transform(X_default.reshape(1, X_default.shape[0]))[0]

    # Determine min/max for knob values
    for i in range(X_scaled.shape[1]):
        if i < total_dummies or i in binary_index_set:
            col_min = 0
            col_max = 1
        else:
            col_min = X_scaled[:, i].min()
            col_max = X_scaled[:, i].max()

            #if X_columnlabels[i] in knobs_mem_catalog:
            #    X_mem[0][i] = mem_max * 1024 * 1024 * 1024  # mem_max GB
            #    col_max = min(col_max, X_scaler.transform(X_mem)[0][i])

            # Set min value to the default value
            # FIXME: support multiple methods can be selected by users
            col_min = X_default_scaled[i]

        X_min[i] = col_min
        X_max[i] = col_max
        X_samples[:, i] = np.random.rand(num_samples) * (col_max - col_min) + col_min

    # Maximize the throughput, moreisbetter
    # Use gradient descent to minimize -throughput
    if not lessisbetter:
        y_scaled = -y_scaled

    q = queue.PriorityQueue()
    for x in range(0, y_scaled.shape[0]):
        q.put((y_scaled[x][0], x))

    i = 0
    while i < TOP_NUM_CONFIG:
        try:
            item = q.get_nowait()
            # Tensorflow get broken if we use the training data points as
            # starting points for GPRGD. We add a small bias for the
            # starting points. GPR_EPS default value is 0.001
            # if the starting point is X_max, we minus a small bias to
            # make sure it is within the range.
            dist = sum(np.square(X_max - X_scaled[item[1]]))
            if dist < 0.001:
                X_samples = np.vstack((X_samples, X_scaled[item[1]] - abs(GPR_EPS)))
            else:
                X_samples = np.vstack((X_samples, X_scaled[item[1]] + abs(GPR_EPS)))
            i = i + 1
        except queue.Empty:
            break

    model = GPRGD(length_scale=DEFAULT_LENGTH_SCALE,
                  magnitude=DEFAULT_MAGNITUDE,
                  max_train_size=MAX_TRAIN_SIZE,
                  batch_size=BATCH_SIZE,
                  num_threads=NUM_THREADS,
                  learning_rate=DEFAULT_LEARNING_RATE,
                  epsilon=DEFAULT_EPSILON,
                  max_iter=MAX_ITER,
                  sigma_multiplier=DEFAULT_SIGMA_MULTIPLIER,
                  mu_multiplier=DEFAULT_MU_MULTIPLIER)
    model.fit(X_scaled, y_scaled, X_min, X_max, ridge=DEFAULT_RIDGE)
    print("constrains_min::::::::", X_min)
    print("constrains_max::::::::", X_max)
    print("train:::::::: ", X_scaled.shape, X_scaled, type(X_scaled[0][0]))
    print("train:::::::: ", y_scaled.shape, y_scaled, type(y_scaled[0][0]))
    print("predict:::::::: ", X_samples.shape, X_samples, type(X_samples[0][0]))
    res = model.predict(X_samples, constraint_helper=constraint_helper)

    best_config_idx = np.argmin(res.minl.ravel())
    best_config = res.minl_conf[best_config_idx, :]
    best_config = X_scaler.inverse_transform(best_config)
    # Decode one-hot encoding into categorical knobs
    best_config = dummy_encoder.inverse_transform(best_config)

    # Although we have max/min limits in the GPRGD training session, it may
    # lose some precisions. e.g. 0.99..99 >= 1.0 may be True on the scaled data,
    # when we inversely transform the scaled data, the different becomes much larger
    # and cannot be ignored. Here we check the range on the original data
    # directly, and make sure the recommended config lies within the range
    X_min_inv = X_scaler.inverse_transform(X_min)
    X_max_inv = X_scaler.inverse_transform(X_max)
    best_config = np.minimum(best_config, X_max_inv)
    best_config = np.maximum(best_config, X_min_inv)

    conf_map = {k: best_config[i] for i, k in enumerate(X_columnlabels)}
    # conf_map_res = {}
    # conf_map_res['status'] = 'good'
    # conf_map_res['recommendation'] = conf_map
    # conf_map_res['info'] = 'INFO: training data size is {}'.format(X_scaled.shape[0])
    return conf_map


