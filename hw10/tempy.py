#!/usr/bin/env python
# coding: utf-8

# In[2]:


import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt
import tempfile
from tensorflow.examples.tutorials.mnist import input_data


# In[2]:



config = tf.ConfigProto()
config.gpu_options.allow_growth = True
sess = tf.Session(config=config)
xtrain = np.load('logistic_xtrain.npy')
ytrain = np.load('logistic_ytrain.npy')

x = tf.placeholder(dtype = tf.float32)
ytrue = tf.placeholder(dtype = tf.float32)
W = tf.Variable(tf.ones([6,1]), dtype = tf.float32)
b = tf.Variable(0, dtype = tf.float32)
z = tf.matmul(x,W) + b
p = 1.0 / (1 + tf.exp(-1*z))
loss = -1*tf.reduce_mean(ytrue*tf.log(p) + (1-ytrue)*tf.log(1-p))

init = tf.global_variables_initializer()
sess.run(init)

optimizer = tf.train.GradientDescentOptimizer(0.1)
train = optimizer.minimize(loss)

losses = np.zeros(10000)
for i in range(10000):
    losses[i] = sess.run(loss, {x:xtrain, ytrue:ytrain})
    sess.run(train, {x:xtrain, ytrue:ytrain})
print(sess.run([W,b]))


# In[3]:


np.mean(losses)


# In[1]:


plt.plot(losses)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




