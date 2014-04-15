pigml
=====

Machine Library Using Pig. The intention of this library is reduce the complexity of using
ML libraries. As of now only colloborative filtering (item similarities) is supported.

How to use this
===============

REGISTER pigml.jar;<br>
DEFINE SIM pig.ml.reco.cf.udf.SIM();<br>

userData = LOAD 'userData' Using PigStorage(',') AS (user:long, item:long, weight:long);<br>
grpdUserData1 = GROUP userData By item;<br>
grpdUserData2 = GROUP userData By item;<br>
crossedData = CROSS grpdUserData1, grpdUserData2;<br>
grpd = GROUP crossedData ALL;<br>
cosineSim = FOREACH grpd GENERATE pig.ml.reco.cf.udf.SIM(crossedData, 2, 'COSINE');<br>
STORE cosineSim INTO 'itemSim';<br>
