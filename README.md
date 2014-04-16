pigml
=====

Machine Library Using Pig. The intention of this library is reduce the complexity of using
ML libraries. As of now only colloborative filtering (item similarities) is supported.

<u>How to use this</u>


<code>REGISTER pigml.jar;</code><br>

<dfn>-- arg1 : Compute similarity if there are at least 2 users, who rated both the products.</dfn><br>
<dfn>-- arg2 : Similarity class to be used.</dfn><br>

<code>DEFINE SIM pig.ml.reco.cf.udf.SIM(2, 'COSINE');</code><br>

<code>userData = LOAD 'data' Using PigStorage(',') AS (user:long, item:chararray, weight:double);</code><br>
<code>grpdUserData1 = GROUP userData By item;</code><br>
<code>grpdUserData2 = GROUP userData By item;</code><br>
<code>crossedData = CROSS grpdUserData1, grpdUserData2;</code><br>
<code>grpd = GROUP crossedData ALL;</code><br>
<code>cosineSim = FOREACH grpd GENERATE SIM(crossedData);</code><br>
<code>STORE cosineSim INTO 'itemSim';</code><br>
