--create the table for question 1, will be populated by the mapreduced file we already made
CREATE TABLE OCT20(
TITLE STRING,
VIEWS INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

--query for question 1, 10 entries top one is the answer
SELECT *
FROM OCT20 
ORDER BY VIEWS DESC LIMIT 10;

--create tables for question 2
--the total views for a page
CREATE TABLE CLICKSTREAMTOTAL(
TITLE STRING,
VIEWS INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

--the total 
CREATE TABLE CLICKSTREAMLINK(
CAME_FROM STRING,
CLICKED INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

CREATE TABLE CLICKSTREAMFINAL(
PAGE_TITLE STRING,
PAGE_VIEWS INT,
LINKS_PRESSED INT,
PERCENTAGE FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT INTO CLICKSTREAMFINAL(
SELECT TITLE, VIEWS, CLICKED, ROUND((CLICKED/VIEWS)*100, 2)
FROM CLICKSTREAMTOTAL
INNER JOIN CLICKSTREAMLINK ON CLICKSTREAMTOTAL.TITLE=CLICKSTREAMLINK.CAME_FROM
);

SELECT *                    --This query is slightly different from what i actually did because this
FROM CLICKSTREAMFINAL       --version is just better for efficiency after
WHERE PAGE_VIEWS > 1000000
ORDER BY PERCENTAGE DESC
LIMIT 10;

--create the unedited clickstream for question 4 so nothing is combined, since this case specifically we dont
--want the info combined
CREATE TABLE CLICKSTREAMUNEDIT(
PREVIOUS PAGE STRING,
CURRENT_PAGE STRING,
LINK_TYPE STRING,
NUMBER_OF_VIEWS INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

--run this multiple times to get final answer, replacing the title in "" where it says previous_page like " "
--with the title you're looking for
SELECT CURRENT_PAGE, NUMBER_OF_VIEWS 
FROM CLICKSTREAMUNEDIT 
WHERE PREVIOUS_PAGE LIKE "Hotel_California" AND LINK_TYPE = "link" AND NOT(PREVIOUS_PAGE = "other-internal" OR PREVIOUS_PAGE="other-search" 
OR PREVIOUS_PAGE="other-external" OR PREVIOUS_PAGE="other-empty" OR PREVIOUS_PAGE="other-other") 
ORDER BY NUMBER_OF_VIEWS DESC
limit 10;

--create tables for question 4
--AU Peak Times
CREATE TABLE AUPEAK(
TITLE STRING,
VIEWS INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

--UK Peak Times
CREATE TABLE UKPEAK(
TITLE STRING,
VIEWS INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

--US Peak Times
CREATE TABLE USPEAK(
TITLE STRING,
VIEWS INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

--query the above tables for highest views, then manually filter for an answer
SELECT *
FROM (AUPEAK/UKPEAK/USPEAK) --only use one of these, remove the other 2
ORDER BY VIEWS DESC
LIMIT 30;

--create tables for the wikihistory
--courtesy of John Rice typing out the 70 columns and saving everyone else a big headache
CREATE TABLE REVISIONS (WIKI_DB STRING,                
                EVENT_ENTITY STRING,			
                EVENT_TYPE STRING,
                EVENT_TIMESTAMP STRING,
                EVENT_COMMENT STRING,
                EVENT_USER_ID INT,
                EVENT_USER_TEXT_HISTORICAL STRING,
                EVENT_USER_TEXT STRING,
                EVENT_USER_BLOCKS_HISTORICAL STRING,
                EVENT_USER_BLOCKS ARRAY<STRING>,
                EVENT_USER_GROUPS_HISTORICAL ARRAY<STRING>,
                EVENT_USER_GROUPS ARRAY<STRING>,
                event_user_is_bot_by_historical ARRAY<STRING>,
                event_user_is_bot_by ARRAY<STRING>,
                event_user_is_created_by_self BOOLEAN,
                event_user_is_created_by_system BOOLEAN,
                event_user_is_created_by_peer BOOLEAN,
                event_user_is_anonymous BOOLEAN,
                event_user_registration_timestamp STRING,
                event_user_creation_timestamp STRING,
                event_user_first_edit_timestamp STRING,
                event_user_revision_count INT,
                event_user_seconds_since_previous_revision INT,
                page_id INT,
                page_title_historical STRING,
                page_title STRING,
                page_namespace_historical INT,
                page_namespace_is_content_historical BOOLEAN,
                page_namespace INT,
                page_namespace_is_content BOOLEAN,
                page_is_redirect BOOLEAN,
                page_is_deleted BOOLEAN,
                page_creation_timestamp STRING,
                page_first_edit_timestamp STRING,
                page_revision_count INT,
                page_seconds_since_previous_revision INT,
                user_id BIGINT,
                user_text_historical STRING,
                user_text STRING,
                user_blocks_historical ARRAY<STRING>,
                user_blocks ARRAY<STRING>,
                user_groups_historical ARRAY<STRING>,
                user_groups ARRAY<String>,
                user_is_bot_by_historical ARRAY<STRING>,
                user_is_bot_by Array<STRING>,
                user_is_created_by_self BOOLEAN,
                user_is_created_by_system boolean,
                user_is_created_by_peer BOOLEAN,
                user_is_anonymous boolean,
                user_registration_timestamp String,
                user_creation_timestamp STRING,
                user_first_edit_timestamp STRING,
                revision_id int,
                revision_parent_id int,
                revision_minor_edit boolean,
                revision_deleted_parts Array<String>,
                revision_deleted_parts_are_suppressed boolean,
                revision_text_bytes int,
                revision_text_bytes_diff int,
                revision_text_sha1 string,
                revision_content_model string,
                revision_content_format string,
                revision_is_deleted_by_page_deletion boolean,
                revision_deleted_by_page_deletion_timestamp string,
                revision_is_identity_reverted boolean,
                revision_first_identity_reverting_revision_id int,
                revision_seconds_to_identity_revert int,
                revision_is_identity_revert boolean,
                revision_is_from_before_page_creation boolean,
                revision_tags Array<string>
                )
 ROW FORMAT DELIMITED 
 FIELDS TERMINATED BY '\t';
 
 --query to find average time to revert vandalism
SELECT WIKI_DB, EVENT_ENTITY, AVG(revision_seconds_to_identity_revert)
FROM REVISIONS
WHERE revision_seconds_to_identity_revert > 600
GROUP BY WIKI_DB, EVENT_ENTITY
LIMIT 10;

--create tables for question 6
CREATE TABLE ENCLICKSTREAM(
EN_TITLE STRING,
EN_VIEWS INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

CREATE TABLE ESCLICKSTREAM(
ES_TITLE STRING,
ES_VIEWS INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

CREATE TABLE DECLICKSTREAM(
DE_TITLE STRING,
DE_VIEWS INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

--SQL query for question 6
SELECT TITLE, TOTAL_VIEWS, EN_VIEWS, ROUND((EN_VIEWS/TOTAL_VIEWS)*100, 2) AS EN_PERCENTAGE, ES_VIEWS, ROUND((ES_VIEWS/TOTAL_VIEWS)*100, 2) AS ES_PERCENTAGE, DE_VIEWS, ROUND((DE_VIEWS/TOTAL_VIEWS)*100, 2) AS DE_PERCENTAGE
 FROM MULTILANGCLICKSTREAM
 INNER JOIN ENCLICKSTREAM ON MULTILANGCLICKSTREAM.TITLE=ENCLICKSTREAM.EN_TITLE
INNER JOIN ESCLICKSTREAM ON MULTILANGCLICKSTREAM.TITLE=ESCLICKSTREAM.ES_TITLE
 INNER JOIN DECLICKSTREAM ON MULTILANGCLICKSTREAM.TITLE=DECLICKSTREAM.DE_TITLE
 ORDER BY TOTAL_VIEWS DESC
 LIMIT 10;

 


