/*
type group
 relations
   define member: [user, group#member]
*/

/*
Sample tuple:
insert into tuples values('group', '1', 'member', 'user', 'jon', '');
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

CREATE MATERIALIZED VIEW group_member_user AS
    SELECT resource_type, resource_id, relation, subject_type, subject_id, subject_relation
    FROM tuples
    WHERE resource_type='group' AND relation='member' AND subject_type='user' AND subject_relation='';

CREATE MATERIALIZED VIEW group_member_group AS
    SELECT resource_type, resource_id, relation, subject_type, subject_id, subject_relation
    FROM tuples
    WHERE resource_type='group' AND relation='member' AND subject_type='group' AND subject_relation='';


DECLARE RECURSIVE VIEW group_member (
    resource_type TEXT,
    resource_id TEXT,         
    relation TEXT, 
    subject_type TEXT,
    subject_id TEXT,
    subject_relation TEXT
);

CREATE MATERIALIZED VIEW group_member AS
    SELECT * FROM group_member_user base
    UNION ALL
    SELECT
        group_member_group.resource_type as resource_type,
        group_member_group.resource_id as resource_id,
        'member' as relation,
        group_member.subject_type as subject_type,
        group_member.subject_id as subject_id,
        group_member.subject_relation as subject_relation
    FROM group_member_group join group_member on group_member_group.subject_id = group_member.resource_id;
