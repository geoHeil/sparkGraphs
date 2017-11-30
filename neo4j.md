# playing around with neo4j

**create the graph**

```
CREATE (Alice:Person {id:'a', fraud:1})
CREATE (Bob:Person {id:'b', fraud:0})
CREATE (Charlie:Person {id:'c', fraud:0})
CREATE (David:Person {id:'d', fraud:0})
CREATE (Esther:Person {id:'e', fraud:0})
CREATE (Fanny:Person {id:'f', fraud:0})
CREATE (Gabby:Person {id:'g', fraud:0})
CREATE (Fraudster:Person {id:'h', fraud:1})


CREATE
  (Alice)-[:CALL {roles:['A']}]->(Bob),
  (Bob)-[:SMS {roles:['B']}]->(Charlie),
  (Charlie)-[:SMS]->(Bob),
  (Fanny)-[:SMS]->(Charlie),
  (Esther)-[:SMS]->(Fanny),
  (Esther)-[:CALL]->(David),
  (David)-[:CALL]->(Alice),
  (David)-[:SMS]->(Esther),
  (Alice)-[:CALL]->(Esther),
  (Alice)-[:CALL]->(Fanny),
  (Fanny)-[:CALL]->(Fraudster)
```


**query**
https://stackoverflow.com/questions/47556306/neo4j-mean-of-property-for-all-friends
```
MATCH p = (source)-[*..3]-(destination)
RETURN source.id, source.fraud, COUNT(*), avg(destination.fraud), COLLECT({id: destination.id, fraud: destination.fraud}) AS neighbors
```

or https://stackoverflow.com/questions/47512635/neo4j-percentage-of-attribute-for-social-network
or https://stackoverflow.com/questions/47554183/neo4j-multiple-match-aggregations-single-pass-over-graph for combined attributes 
