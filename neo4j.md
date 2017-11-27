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
```
MATCH (a)-->(b)
WHERE b.fraud = 1
RETURN (count() / ( MATCH (a) -->(b) RETURN count() ) * 100)
```
