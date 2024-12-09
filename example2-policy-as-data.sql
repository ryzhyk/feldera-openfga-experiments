/*
A generic FGA engine that can evaluate any number of 2-ary rules of the form:

(object1, relation1, object2) AND (object2, relation2, object3) => (object1, relation3, object3)

modeled as records in the `rules` table.  Note that this implementation does not yet support n-ary
rules where n!=2 and rules with negations.

The datagen is configured to populate the tables with a single relation called MEMBER and a single
rule: (object1, MEMBER, object2) AND (object2, MEMBER, object3) => (object1, MEMBER, object3).

*/

CREATE TABLE relation(
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL
) WITH (
    'materialized' = 'true',
    'connectors' = '[{
      "transport": {
        "name": "datagen",
        "config": {
            "plan": [{
                "limit": 1,
                "fields": {
                    "id": { "values": [1] },
                    "name": { "values": ["MEMBER"] }
                }
            }]
        }
      }
    }]'
);

CREATE TABLE rules (
    id INT NOT NULL PRIMARY KEY,
    path_relation VARCHAR NOT NULL,
    edge_relation VARCHAR NOT NULL,
    derived_relation VARCHAR NOT NULL
) WITH (
    'materialized' = 'true',
    'connectors' = '[{
      "transport": {
        "name": "datagen",
        "config": {
            "plan": [{
                "limit": 1,
                "fields": {
                    "path_relation": { "values": ["MEMBER"] },
                    "edge_relation": { "values": ["MEMBER"] },
                    "derived_relation": { "values": ["MEMBER"] }
                }
            }]
        }
      }
    }]'
);

CREATE TABLE object_edges(
    object1 BIGINT NOT NULL,
    object2 BIGINT NOT NULL,
    relation INT NOT NULL
) WITH (
    'materialized' = 'true',
    'connectors' = '[{
      "transport": {
        "name": "datagen",
        "config": {
            "plan": [
            {
                "limit": 0,
                "fields": {
                    "object1": { "values": [] },
                    "object2": { "values": [] },
                    "relation": { "values": [] }
                }
            }]
        }
      }
    }]'
);


CREATE TABLE user_edges(
    userid BIGINT NOT NULL,
    objectid BIGINT NOT NULL,
    relation INT NOT NULL
) WITH (
    'materialized' = 'true',
    'connectors' = '[{
      "transport": {
        "name": "datagen",
        "config": {
            "plan": [{
                "fields": {
                    "userid": { "values": [1] },
                    "objectid": { "values": [2] },
                    "relation": { "values": [1] }
                }
            }]
        }
      }
    }]'

);

CREATE MATERIALIZED VIEW edges AS
SELECT
    userid as object1,
    objectid as object2,
    relation
FROM user_edges
UNION ALL SELECT * FROM object_edges;

-- Resolve relation names into id's in rules.
CREATE MATERIALIZED VIEW resolved_rules AS
SELECT
    rel1.id as path_relation,
    rel2.id as edge_relation,
    rel3.id as derived_relation
FROM rules
JOIN relation as rel1 on rules.path_relation = rel1.name
JOIN relation as rel2 on rules.edge_relation = rel2.name
JOIN relation as rel3 on rules.derived_relation = rel3.name;

-- Compute transitive closure of `rules` over the object-relation graph
-- defined by `edges`.
DECLARE RECURSIVE VIEW relationships (
    object1 BIGINT NOT NULL,
    object2 BIGINT NOT NULL,
    relation INT NOT NULL
);

CREATE MATERIALIZED VIEW suffixes AS
SELECT
    resolved_rules.path_relation,
    resolved_rules.derived_relation,
    edges.object1,
    edges.object2
FROM
    resolved_rules JOIN edges on resolved_rules.edge_relation = edges.relation;

CREATE MATERIALIZED VIEW relationships
AS
    SELECT * FROM edges
    UNION ALL
    SELECT
        relationships.object1 as object1,
        suffixes.object2 as object2,
        suffixes.derived_relation as relation
    FROM
        relationships
        JOIN suffixes ON relationships.relation = suffixes.path_relation AND relationships.object2 = suffixes.object1;
