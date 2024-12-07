/*
model
 schema 1.1

type user

type folder
 relations
   define parent: [folder]
   define viewer: [user] or viewer from parent


type document
 relations
   define parent1: [folder]
   define parent2: [folder]
   define viewer: viewer from parent1 or viewer from parent2
*/

/*
Sample input:

insert into tuples values('folder', '1a', 'parent', 'folder', '1')
insert into tuples values('folder', '1b', 'parent', 'folder', '1')
insert into tuples values('folder', '1c', 'parent', 'folder', '1')
insert into tuples values('folder', '2a', 'parent', 'folder', '2')
insert into tuples values('folder', '2b', 'parent', 'folder', '2')
insert into tuples values('folder', '2c', 'parent', 'folder', '2')
insert into tuples values('document', '1', 'parent1', 'folder', '1a')
insert into tuples values('document', '1', 'parent1', 'folder', '1b')
insert into tuples values('document', '1', 'parent1', 'folder', '1c')
insert into tuples values('document', '1', 'parent2', 'folder', '2a')
insert into tuples values('document', '1', 'parent2', 'folder', '2b')
insert into tuples values('document', '1', 'parent2', 'folder', '2c')
insert into tuples values('folder', '1', 'viewer', 'user', 'jon')
insert into tuples values('folder', '2', 'viewer', 'user', 'bob')

*/


CREATE TABLE tuples (
    resource_type string,
    resource_id string,
    relation string,
    subject_type string,
    subject_id string
) WITH (
    'materialized' = 'true'
);

CREATE MATERIALIZED VIEW folder_parent_folder AS
    SELECT resource_type, resource_id, relation, subject_type, subject_id
    FROM tuples
    WHERE resource_type='folder' AND relation='parent' AND subject_type='folder';

CREATE MATERIALIZED VIEW folder_viewer_user AS
    SELECT resource_type, resource_id, relation, subject_type, subject_id
    FROM tuples
    WHERE resource_type='folder' AND relation='viewer' AND subject_type='user';

CREATE MATERIALIZED VIEW document_parent1_folder AS
    SELECT resource_type, resource_id, relation, subject_type, subject_id
    FROM tuples
    WHERE resource_type='document' AND relation='parent1' AND subject_type='folder';

CREATE MATERIALIZED VIEW document_parent2_folder AS
    SELECT resource_type, resource_id, relation, subject_type, subject_id
    FROM tuples
    WHERE resource_type='document' AND relation='parent2' AND subject_type='folder';

DECLARE RECURSIVE VIEW folder_viewer (
    resource_type string,
    resource_id string,
    relation string,
    subject_type string,
    subject_id string
);

CREATE MATERIALIZED VIEW folder_viewer AS
    SELECT * FROM folder_viewer_user
    UNION ALL
    SELECT 
        folder_parent_folder.resource_type,
        folder_parent_folder.resource_id,
        'user' as relation,
        folder_viewer.subject_type,
        folder_viewer.subject_id
    FROM
        folder_viewer join folder_parent_folder on folder_viewer.resource_id = folder_parent_folder.subject_id;

CREATE MATERIALIZED VIEW document_viewer AS
(SELECT
    document_parent1_folder.resource_type,
    document_parent1_folder.resource_id,
    'viewer' as relation,
    folder_viewer.subject_type,
    folder_viewer.subject_id
 FROM
    document_parent1_folder join folder_viewer on document_parent1_folder.subject_id = folder_viewer.resource_id)
UNION
(SELECT
    document_parent2_folder.resource_type,
    document_parent2_folder.resource_id,
    'viewer' as relation,
    folder_viewer.subject_type,
    folder_viewer.subject_id
 FROM
    document_parent2_folder join folder_viewer on document_parent2_folder.subject_id = folder_viewer.resource_id);
