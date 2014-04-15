pigml
=====

Machine Library Using Pig. The intention of this library is reduce the complexity of using
ML libraries. As of now only colloborative filtering (item similarities) is supported.

How to use this
===============

<code>
REGISTER pigml.jar;
DEFINE SIM pig.ml.reco.cf.udf.SIM();

userData = LOAD 'userData' Using PigStorage(',') AS (user:long, item:long, weight:long);
grpdUserData1 = GROUP userData By item;
grpdUserData2 = GROUP userData By item;
crossedData = CROSS grpdUserData1, grpdUserData2;
grpd = GROUP crossedData ALL;
cosineSim = FOREACH grpd GENERATE pig.ml.reco.cf.udf.SIM(crossedData, 2, 'COSINE');
STORE cosineSim INTO 'itemSim';
</code>