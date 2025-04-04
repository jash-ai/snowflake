-- REFERENCE HERE: https://wiki.voror.co.uk/index.php?title=Compass_2.1_Schema_Documentation#concept

select TOP 20
PATIENT_ID,
PERSON_ID ,-- THIS IS NHS_NUMBER AND NOT PERSON_ID IN THE ICARE/CERNER MODEL SENSE
ENCOUNTER_ID,
CLINICAL_EFFECTIVE_DATE,
RESULT_VALUE,
RESULT_TEXT,
RESULT_DATE,
RESULT_TEXT,
NON_CORE_CONCEPT_ID,
CORE_CONCEPT_ID,
CORE_CONCEPT_ID_RAW,
RESULT_CONCEPT_ID,
RESULT_CONCEPT_ID_RAW,
IS_PROBLEM,
IS_REVIEW,
DATE_RECORDED,
IS_DELETED,
IS_CONFIDENTIAL
from 
COMPASS.PUBLIC.OBSERVATION


select
TOP 5
COUNT(*) AS COUNTALL,
CORE_CONCEPT_ID_RAW
from 
COMPASS.PUBLIC.OBSERVATION
WHERE COALESCE(TRY_CAST(CORE_CONCEPT_ID_RAW AS VARCHAR(16777216)),'-1')<>'-1'
GROUP BY 
CORE_CONCEPT_ID_RAW
ORDER BY 
COUNT(*) DESC

-- TOP 5 CORE CONCEPTS in the observation(s) table
-- are these snomed if so then:

--10667	163020007
--10605	72313002
--10595	1091811000000102
--4190	1572871000006101
--3139	225323000

-- 163020007,72313002,1091811000000102,1572871000006101,225323000

--163020007: This code corresponds to "Pulse oximetry (procedure)".
--72313002: This code represents "Systolic arterial pressure (observable entity)". 
--1091811000000102: This code denotes "Diastolic arterial pressure (observable entity)". 
--1572871000006101: This code is associated with "COVID-19 vaccination first dose declined (situation)".
--225323000: This code stands for "Blood pressure reading (observable entity)".
