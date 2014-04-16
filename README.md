pigml
=====

Machine Library Using Pig. The intention of this library is reduce the complexity of using
ML libraries. As of now only colloborative filtering (item similarities) is supported.

<u>How to use this</u>


REGISTER pigml.jar;<br>

-- arg1 : Compute similarity if there are at least 2 users, who rated both the products.
-- arg2 : Similarity class to be used.

DEFINE SIM pig.ml.reco.cf.udf.SIM(2, 'COSINE');<br>

userData = LOAD 'data' Using PigStorage(',') AS (user:long, item:chararray, weight:double);<br>
grpdUserData1 = GROUP userData By item;<br>
grpdUserData2 = GROUP userData By item;<br>
crossedData = CROSS grpdUserData1, grpdUserData2;<br>
grpd = GROUP crossedData ALL;<br>
cosineSim = FOREACH grpd GENERATE SIM(crossedData);<br>
STORE cosineSim INTO 'itemSim';<br>