# pigml

Machine Library Using Pig. The intention of this library is reduce the complexity of using
ML libraries. As of now only colloborative filtering (item similarities) is supported.

## How to use this

```
REGISTER pigml.jar;

-- arg1 : Compute similarity if there are at least 2 users, who rated both the products.
-- arg2 : Similarity class to be used.
DEFINE SIM pig.ml.reco.cf.udf.SIM(2, 'COSINE');

userData = LOAD 'data' Using PigStorage(',') AS (user:long, item:chararray, weight:double);
grpdUserData1 = GROUP userData By item;
grpdUserData2 = GROUP userData By item;
crossedData = CROSS grpdUserData1, grpdUserData2;
grpd = GROUP crossedData ALL;
cosineSim = FOREACH grpd GENERATE SIM(crossedData);
STORE cosineSim INTO 'itemSim';
```
