/*
The following input causes the pipeline to diverge, as expected:

insert into tuples values ('document', '1', 'viewer', 'user', 'jon', '');
insert into tuples values('document', '1', 'restricted', 'user', 'jon', '')
*/


CREATE TABLE tuples (
    resource_type string,
    resource_id string,
    relation string,
    subject_type string,
    subject_id string,
    subject_relation string
) WITH (
    'materialized' = 'true'
);

CREATE MATERIALIZED VIEW document_viewer_user AS
    SELECT resource_type, resource_id, relation, subject_type, subject_id, subject_relation
    FROM tuples
    WHERE resource_type='document' AND relation='viewer' AND subject_type='user' AND subject_relation='';

CREATE MATERIALIZED VIEW document_restricted_user AS
    SELECT resource_type, resource_id, relation, subject_type, subject_id, subject_relation
    FROM tuples
    WHERE resource_type='document' AND relation='restricted' AND subject_type='user' AND subject_relation='';

CREATE MATERIALIZED VIEW document_unrestricted_user AS 
    SELECT resource_type, resource_id, relation, subject_type, subject_id, subject_relation
    FROM tuples
    WHERE resource_type='document' AND relation='unrestricted' AND subject_type='user' AND subject_relation='';

DECLARE RECURSIVE VIEW document_viewer (
    resource_type TEXT,
    resource_id TEXT,         
    relation TEXT, 
    subject_type TEXT,
    subject_id TEXT,
    subject_relation TEXT
);

DECLARE RECURSIVE VIEW document_restricted (
    resource_type TEXT,
    resource_id TEXT, 
    relation TEXT, 
    subject_type TEXT,
    subject_id TEXT,
    subject_relation TEXT
);

DECLARE RECURSIVE VIEW document_unrestricted (
    resource_type TEXT,
    resource_id TEXT, 
    relation TEXT, 
    subject_type TEXT,
    subject_id TEXT,
    subject_relation TEXT
);


CREATE MATERIALIZED VIEW document_viewer AS
    SELECT * FROM document_viewer_user base
    WHERE NOT EXISTS (
        SELECT * FROM document_restricted sub WHERE base.resource_id = sub.resource_id
    );

CREATE MATERIALIZED VIEW document_restricted AS
    SELECT * FROM document_restricted_user base
    WHERE NOT EXISTS (
        SELECT * FROM document_unrestricted sub WHERE base.resource_id = sub.resource_id
    );

CREATE MATERIALIZED VIEW document_unrestricted AS
    SELECT * FROM document_unrestricted_user base
    UNION ALL
    SELECT * FROM document_viewer;
