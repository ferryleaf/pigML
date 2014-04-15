pigml
=====

Machine Library Using Pig. The intention of this library is reduce the complexity of using
ML libraries. As of now only colloborative filtering (item similarities) is supported.

How to use this
===============

<code>REGISTER pigml.jar;<code>
<code>DEFINE SIM pig.ml.reco.cf.udf.SIM();</code>

<code>userData = LOAD 'userData' Using PigStorage(',') AS (user:long, item:long, weight:long);</code>
<code>grpdUserData1 = GROUP userData By item;</code>
<code>grpdUserData2 = GROUP userData By item;</code>
<code>crossedData = CROSS grpdUserData1, grpdUserData2;</code>
<code>grpd = GROUP crossedData ALL;</code>
<code>cosineSim = FOREACH grpd GENERATE pig.ml.reco.cf.udf.SIM(crossedData, 2, 'COSINE');</code>
<code>STORE cosineSim INTO 'itemSim';</code>