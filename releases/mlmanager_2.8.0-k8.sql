ALTER TABLE MLManager.Artifacts ALTER COLUMN file_extension SET DATA TYPE VARCHAR(25);

-- Can't drop/rename a table that is the target of a constraint, can't remove a constraint you don't have the name of, so you have to remove the whole table - ripple effect ensues
CREATE TABLE FEATURESTORE.DEPLOYMENT_FEATURE_STATS_TMP (
    MODEL_SCHEMA_NAME VARCHAR(128) NOT NULL,
    MODEL_TABLE_NAME VARCHAR(128) NOT NULL,
    FEATURE_ID INTEGER NOT NULL,
    MODEL_START_TS TIMESTAMP,
    MODEL_END_TS TIMESTAMP,
    FEATURE_CARDINALITY INTEGER,
    FEATURE_HISTOGRAM CLOB(2147483647),
    FEATURE_MEAN DECIMAL(31,0),
    FEATURE_MEDIAN DECIMAL(31,0),
    FEATURE_COUNT INTEGER,
    FEATURE_STDDEV DECIMAL(31,0),
    PRIMARY KEY(MODEL_SCHEMA_NAME, MODEL_TABLE_NAME, FEATURE_ID),
    CONSTRAINT fk_deployment_feature_stats_feature FOREIGN KEY (FEATURE_ID) REFERENCES FEATURESTORE.FEATURE(FEATURE_ID) ON UPDATE NO ACTION ON DELETE NO ACTION
);
INSERT INTO FEATURESTORE.DEPLOYMENT_FEATURE_STATS_TMP (model_schema_name, model_table_name, feature_id, model_start_ts, model_end_ts, feature_cardinality, feature_histogram, feature_mean, feature_median, feature_count, feature_stddev)
SELECT model_schema_name, model_table_name, feature_id, model_start_ts, model_end_ts, feature_cardinality, feature_histogram, feature_mean, feature_median, feature_count, feature_stddev
FROM FEATURESTORE.DEPLOYMENT_FEATURE_STATS_TMP;
DROP TABLE FEATURESTORE.DEPLOYMENT_FEATURE_STATS;
RENAME TABLE FEATURESTORE.DEPLOYMENT_FEATURE_STATS_TMP to DEPLOYMENT_FEATURE_STATS;

-- We do this for the history table instead of just dropping and adding columns because it brings very weird behavior
CREATE TABLE FEATURESTORE.DEPLOYMENT_HISTORY_TMP (
    MODEL_SCHEMA_NAME VARCHAR(128) NOT NULL,
    MODEL_TABLE_NAME VARCHAR(128) NOT NULL,
    ASOF_TS TIMESTAMP NOT NULL,
    TRAINING_SET_ID INTEGER,
    TRAINING_SET_VERSION BIGINT,
    RUN_ID VARCHAR(32),
    LAST_UPDATE_TS TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    LAST_UPDATE_USERNAME VARCHAR(128) NOT NULL DEFAULT CURRENT_USER,
    PRIMARY KEY(MODEL_SCHEMA_NAME, MODEL_TABLE_NAME, ASOF_TS),
    CONSTRAINT fk_deployment_history_runs FOREIGN KEY (RUN_ID) REFERENCES MLMANAGER.RUNS(RUN_UUID) ON UPDATE NO ACTION ON DELETE NO ACTION
);
INSERT INTO FeatureStore.DEPLOYMENT_HISTORY_TMP (model_schema_name, model_table_name, asof_ts, training_set_id, run_id, last_update_username, last_update_ts)
SELECT model_schema_name, model_table_name, asof_ts, training_set_id, run_id, last_update_username, last_update_ts
FROM FeatureStore.Deployment_History;
DROP TABLE featurestore.deployment_history;
RENAME TABLE FeatureStore.DEPLOYMENT_HISTORY_TMP to DEPLOYMENT_HISTORY;

CREATE TABLE FEATURESTORE.DEPLOYMENT_TMP (
    MODEL_SCHEMA_NAME VARCHAR(128) NOT NULL,
    MODEL_TABLE_NAME VARCHAR(128) NOT NULL,
    TRAINING_SET_ID INTEGER,
    TRAINING_SET_VERSION BIGINT,
    RUN_ID VARCHAR(32),
    LAST_UPDATE_TS TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    LAST_UPDATE_USERNAME VARCHAR(128) NOT NULL DEFAULT CURRENT_USER,
    PRIMARY KEY(MODEL_SCHEMA_NAME, MODEL_TABLE_NAME),
    CONSTRAINT fk_deployment_runs FOREIGN KEY (RUN_ID) REFERENCES MLMANAGER.RUNS(RUN_UUID) ON UPDATE NO ACTION ON DELETE NO ACTION
);
INSERT INTO FEATURESTORE.DEPLOYMENT_TMP (model_schema_name, model_table_name, training_set_id, run_id, last_update_username, last_update_ts)
SELECT model_schema_name, model_table_name, training_set_id, run_id, last_update_username, last_update_ts
FROM FEATURESTORE.DEPLOYMENT;
DROP TABLE FEATURESTORE.DEPLOYMENT;
RENAME TABLE FEATURESTORE.DEPLOYMENT_TMP to DEPLOYMENT;
ALTER TABLE FEATURESTORE.DEPLOYMENT_FEATURE_STATS ADD CONSTRAINT fk_deployment_feature_stats_deployment FOREIGN KEY (MODEL_SCHEMA_NAME, MODEL_TABLE_NAME) REFERENCES FEATURESTORE.DEPLOYMENT(MODEL_SCHEMA_NAME, MODEL_TABLE_NAME) ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE FEATURESTORE.DEPLOYMENT_HISTORY ADD CONSTRAINT fk_deployment_history_deployment FOREIGN KEY (MODEL_SCHEMA_NAME, MODEL_TABLE_NAME) REFERENCES FEATURESTORE.DEPLOYMENT(MODEL_SCHEMA_NAME, MODEL_TABLE_NAME) ON UPDATE NO ACTION ON DELETE NO ACTION;

CREATE TRIGGER FeatureStore.deployment_historian
AFTER UPDATE
ON FeatureStore.deployment
REFERENCING OLD AS od
FOR EACH ROW
INSERT INTO FeatureStore.deployment_history ( model_schema_name, model_table_name, asof_ts, training_set_id, training_set_version, run_id, last_update_ts, last_update_username)
VALUES ( od.model_schema_name, od.model_table_name, CURRENT_TIMESTAMP, od.training_set_id, od.training_set_version, od.run_id, od.last_update_ts, od.last_update_username);

CREATE TABLE FEATURESTORE.TRAINING_SET_FEATURE_STATS_TMP (
    TRAINING_SET_ID INTEGER NOT NULL,
    TRAINING_SET_VERSION BIGINT NOT NULL,
    FEATURE_ID INTEGER NOT NULL,
    FEATURE_CARDINALITY INTEGER,
    FEATURE_HISTOGRAM CLOB(2147483647),
    FEATURE_MEAN DECIMAL(31,0),
    FEATURE_MEDIAN DECIMAL(31,0),
    FEATURE_COUNT INTEGER,
    FEATURE_STDDEV DECIMAL(31,0),
    LAST_UPDATE_TS TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    LAST_UPDATE_USERNAME VARCHAR(128) NOT NULL DEFAULT CURRENT_USER,
    PRIMARY KEY(TRAINING_SET_ID, TRAINING_SET_VERSION, FEATURE_ID), 
    CONSTRAINT fk_training_set_feature_stats_feature FOREIGN KEY (FEATURE_ID) REFERENCES FEATURESTORE.FEATURE(FEATURE_ID) ON UPDATE NO ACTION ON DELETE NO ACTION
);
INSERT INTO FEATURESTORE.TRAINING_SET_FEATURE_STATS_TMP (training_set_id, training_set_version, feature_id, feature_cardinality, feature_histogram, feature_mean, feature_median, feature_count, feature_stddev, last_update_ts, last_update_username)
SELECT training_set_id, 1, feature_id, feature_cardinality, feature_histogram, feature_mean, feature_median, feature_count, feature_stddev, last_update_ts, last_update_username
FROM FEATURESTORE.TRAINING_SET_FEATURE_STATS_TMP;
DROP TABLE FEATURESTORE.TRAINING_SET_FEATURE_STATS;
RENAME TABLE FEATURESTORE.TRAINING_SET_FEATURE_STATS_TMP to TRAINING_SET_FEATURE_STATS;

CREATE TABLE FEATURESTORE.TRAINING_SET_LABEL_STATS_TMP (
    TRAINING_SET_ID INTEGER NOT NULL,
    TRAINING_SET_VERSION BIGINT NOT NULL,
    LABEL_COLUMN INTEGER NOT NULL,
    LABEL_CARDINALITY INTEGER,
    LABEL_HISTOGRAM CLOB(2147483647),
    LABEL_MEAN DECIMAL(31,0),
    LABEL_MEDIAN DECIMAL(31,0),
    LABEL_COUNT INTEGER,
    LABEL_STDDEV DECIMAL(31,0),
    LAST_UPDATE_TS TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    LAST_UPDATE_USERNAME VARCHAR(128) NOT NULL DEFAULT CURRENT_USER,
    PRIMARY KEY(TRAINING_SET_ID, TRAINING_SET_VERSION, LABEL_COLUMN)
);
INSERT INTO FEATURESTORE.TRAINING_SET_LABEL_STATS_TMP (training_set_id, training_set_version, label_column, label_cardinality, label_histogram, label_mean, label_median, label_count, label_stddev, last_update_ts, last_update_username)
SELECT training_set_id, training_set_version, label_column, label_cardinality, label_histogram, label_mean, label_median, label_count, label_stddev, last_update_ts, last_update_username
FROM FEATURESTORE.TRAINING_SET_LABEL_STATS;
DROP TABLE FEATURESTORE.TRAINING_SET_LABEL_STATS;
RENAME TABLE FEATURESTORE.TRAINING_SET_LABEL_STATS_TMP to TRAINING_SET_LABEL_STATS;

CREATE TABLE FEATURESTORE.TRAINING_SET_INSTANCE_TMP (
    TRAINING_SET_ID INTEGER NOT NULL,
    TRAINING_SET_VERSION BIGINT NOT NULL,
    TRAINING_SET_START_TS TIMESTAMP,
    TRAINING_SET_END_TS TIMESTAMP,
    TRAINING_SET_CREATE_TS TIMESTAMP,
    LAST_UPDATE_TS TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    LAST_UPDATE_USERNAME VARCHAR(128) NOT NULL DEFAULT CURRENT_USER, 
    PRIMARY KEY(TRAINING_SET_ID, TRAINING_SET_VERSION)
);
INSERT INTO FEATURESTORE.TRAINING_SET_INSTANCE_TMP (training_set_id, training_set_version, training_set_start_ts, training_set_end_ts, training_set_create_ts, last_update_ts, last_update_username)
SELECT training_set_id, training_set_version, training_set_start_ts, training_set_end_ts, training_set_create_ts, last_update_ts, last_update_username
FROM FEATURESTORE.TRAINING_SET_INSTANCE;
DROP TABLE FEATURESTORE.TRAINING_SET_INSTANCE;
RENAME TABLE FEATURESTORE.TRAINING_SET_INSTANCE_TMP TO TRAINING_SET_INSTANCE;
ALTER TABLE FEATURESTORE.DEPLOYMENT ADD CONSTRAINT fk_deployment_training_set_instance FOREIGN KEY (TRAINING_SET_ID, TRAINING_SET_VERSION) REFERENCES FEATURESTORE.TRAINING_SET_INSTANCE(TRAINING_SET_ID, TRAINING_SET_VERSION) ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE FEATURESTORE.TRAINING_SET_FEATURE_STATS ADD CONSTRAINT fk_training_set_feature_stats_training_set_instance FOREIGN KEY (TRAINING_SET_ID, TRAINING_SET_VERSION) REFERENCES FEATURESTORE.TRAINING_SET_INSTANCE(TRAINING_SET_ID, TRAINING_SET_VERSION) ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE FEATURESTORE.TRAINING_SET_LABEL_STATS ADD CONSTRAINT fk_training_set_label_stats_training_set_instance FOREIGN KEY (TRAINING_SET_ID, TRAINING_SET_VERSION) REFERENCES FEATURESTORE.TRAINING_SET_INSTANCE(TRAINING_SET_ID, TRAINING_SET_VERSION) ON UPDATE NO ACTION ON DELETE NO ACTION;

CREATE TABLE FEATURESTORE.TRAINING_SET_FEATURE_TMP (
    TRAINING_SET_ID INTEGER NOT NULL,
    FEATURE_ID INTEGER NOT NULL,
    IS_LABEL SMALLINT,
    LAST_UPDATE_TS TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    LAST_UPDATE_USERNAME VARCHAR(128) NOT NULL DEFAULT CURRENT_USER,
    PRIMARY KEY(TRAINING_SET_ID, FEATURE_ID),
    CONSTRAINT fk_training_set_feature_feature FOREIGN KEY (FEATURE_ID) REFERENCES FEATURESTORE.FEATURE(FEATURE_ID) ON UPDATE NO ACTION ON DELETE NO ACTION
);
INSERT INTO FEATURESTORE.TRAINING_SET_FEATURE_TMP (training_set_id, feature_id, is_label, last_update_ts, last_update_username)
SELECT training_set_id, feature_id, is_label, last_update_ts, last_update_username
FROM FEATURESTORE.TRAINING_SET_FEATURE;
DROP TABLE FEATURESTORE.TRAINING_SET_FEATURE;
RENAME TABLE FEATURESTORE.TRAINING_SET_FEATURE_TMP TO TRAINING_SET_FEATURE;
ALTER TABLE FEATURESTORE.TRAINING_SET_FEATURE ADD CONSTRAINT check_is_label CHECK (is_label IN (0, 1));

CREATE TABLE FEATURESTORE.TRAINING_VIEW_VERSION (
    VIEW_ID INTEGER NOT NULL,
    VIEW_VERSION INTEGER NOT NULL,
    SQL_TEXT CLOB(2147483647),
    LABEL_COLUMN VARCHAR(128),
    TS_COLUMN VARCHAR(128),
    LAST_UPDATE_TS TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    LAST_UPDATE_USERNAME VARCHAR(128) NOT NULL DEFAULT CURRENT_USER,
    PRIMARY KEY(VIEW_ID, VIEW_VERSION),
    CONSTRAINT fk_training_view_version_training_view FOREIGN KEY (VIEW_ID) REFERENCES FEATURESTORE.TRAINING_VIEW(VIEW_ID) ON UPDATE NO ACTION ON DELETE NO ACTION
);
INSERT INTO FEATURESTORE.TRAINING_VIEW_VERSION (view_id, view_version, sql_text, label_column, ts_column, last_update_ts, last_update_username)
SELECT view_id, 1, sql_text, label_column, ts_column, last_update_ts, last_update_username
FROM FEATURESTORE.TRAINING_VIEW;
ALTER TABLE FEATURESTORE.TRAINING_VIEW DROP COLUMN SQL_TEXT;
ALTER TABLE FEATURESTORE.TRAINING_VIEW DROP COLUMN LABEL_COLUMN;
ALTER TABLE FEATURESTORE.TRAINING_VIEW DROP COLUMN TS_COLUMN;
ALTER TABLE FEATURESTORE.TRAINING_VIEW DROP COLUMN LAST_UPDATE_TS;
ALTER TABLE FEATURESTORE.TRAINING_VIEW DROP COLUMN LAST_UPDATE_USERNAME;

CREATE TABLE FEATURESTORE.TRAINING_VIEW_KEY_TMP (
    VIEW_ID INTEGER NOT NULL,
    VIEW_VERSION INTEGER NOT NULL,
    KEY_COLUMN_NAME VARCHAR(128) NOT NULL,
    KEY_TYPE VARCHAR(1) NOT NULL,
    LAST_UPDATE_TS TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    LAST_UPDATE_USERNAME VARCHAR(128) NOT NULL DEFAULT CURRENT_USER,
    PRIMARY KEY(VIEW_ID, VIEW_VERSION, KEY_COLUMN_NAME, KEY_TYPE),
    CONSTRAINT fk_training_view_key_training_view_version FOREIGN KEY (VIEW_ID, VIEW_VERSION) REFERENCES FEATURESTORE.TRAINING_VIEW_VERSION(VIEW_ID, VIEW_VERSION) ON UPDATE NO ACTION ON DELETE NO ACTION
);
INSERT INTO FEATURESTORE.TRAINING_VIEW_KEY_TMP (view_id, view_version, key_column_name, key_type, last_update_ts, last_update_username)
SELECT view_id, 1, key_column_name, key_type, last_update_ts, last_update_username
FROM FEATURESTORE.TRAINING_VIEW_KEY;
DROP TABLE FEATURESTORE.TRAINING_VIEW_KEY;
RENAME TABLE FEATURESTORE.TRAINING_VIEW_KEY_TMP TO TRAINING_VIEW_KEY;
ALTER TABLE FEATURESTORE.TRAINING_VIEW_KEY ADD CONSTRAINT check_key_type CHECK (key_type IN ('P', 'J'));

CREATE TABLE FEATURESTORE.TRAINING_SET_TMP (
    TRAINING_SET_ID INTEGER NOT NULL GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1),
    NAME VARCHAR(255),
    VIEW_ID INTEGER,
    VIEW_VERSION INTEGER,
    LAST_UPDATE_TS TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    LAST_UPDATE_USERNAME VARCHAR(128) NOT NULL DEFAULT CURRENT_USER,
    PRIMARY KEY(TRAINING_SET_ID),
    CONSTRAINT fk_training_set_training_view_version FOREIGN KEY (VIEW_ID, VIEW_VERSION) REFERENCES FEATURESTORE.TRAINING_VIEW_VERSION(VIEW_ID, VIEW_VERSION) ON UPDATE NO ACTION ON DELETE NO ACTION
);
INSERT INTO FEATURESTORE.TRAINING_SET_TMP (training_set_id, name, view_id, view_version, last_update_ts, last_update_username)
SELECT training_set_id, name, view_id, 1, last_update_ts, last_update_username
FROM FEATURESTORE.TRAINING_SET;
DROP TABLE FEATURESTORE.TRAINING_SET;
RENAME TABLE FEATURESTORE.TRAINING_SET_TMP TO TRAINING_SET;
ALTER TABLE FEATURESTORE.TRAINING_SET_INSTANCE ADD CONSTRAINT fk_training_set_instance_training_set FOREIGN KEY (TRAINING_SET_ID) REFERENCES FEATURESTORE.TRAINING_SET(TRAINING_SET_ID) ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE FEATURESTORE.TRAINING_SET_FEATURE ADD CONSTRAINT fk_training_set_feature_training_set FOREIGN KEY (TRAINING_SET_ID) REFERENCES FEATURESTORE.TRAINING_SET(TRAINING_SET_ID) ON UPDATE NO ACTION ON DELETE NO ACTION;

CREATE TABLE FEATURESTORE.FEATURE_SET_VERSION (
    FEATURE_SET_ID INTEGER,
    FEATURE_SET_VERSION INTEGER,
    IS_LIVE SMALLINT DEFAULT 1 NOT NULL,
    DEPLOYED SMALLINT DEFAULT 0,
    DEPLOY_TS TIMESTAMP,
    CREATE_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CREATE_USERNAME VARCHAR(128) NOT NULL DEFAULT CURRENT_USER,
    PRIMARY KEY (FEATURE_SET_ID, FEATURE_SET_VERSION),
    CONSTRAINT fk_feature_set_version_feature_set FOREIGN KEY (FEATURE_SET_ID) REFERENCES FEATURESTORE.FEATURE_SET(FEATURE_SET_ID)
);
INSERT INTO FEATURESTORE.FEATURE_SET_VERSION (feature_set_id, feature_set_version, is_live, deployed, deploy_ts, create_ts, create_username)
SELECT feature_set_id, 1, 1, deployed, deploy_ts, last_update_ts, last_update_username
from FEATURESTORE.FEATURE_SET;
ALTER TABLE FEATURESTORE.FEATURE_SET DROP COLUMN DEPLOYED;
ALTER TABLE FEATURESTORE.FEATURE_SET DROP COLUMN DEPLOY_TS;

ALTER TABLE FEATURESTORE.PENDING_FEATURE_SET_DEPLOYMENT ADD COLUMN FEATURE_SET_VERSION INTEGER;
ALTER TABLE FEATURESTORE.PENDING_FEATURE_SET_DEPLOYMENT DROP PRIMARY KEY;
ALTER TABLE FEATURESTORE.PENDING_FEATURE_SET_DEPLOYMENT ADD PRIMARY KEY(FEATURE_SET_ID, FEATURE_SET_VERSION);
ALTER TABLE FEATURESTORE.PENDING_FEATURE_SET_DEPLOYMENT DROP FOREIGN KEY fk_feature_set_id;
ALTER TABLE FEATURESTORE.PENDING_FEATURE_SET_DEPLOYMENT ADD CONSTRAINT fk_pending_feature_set_deployment_feature_set_version FOREIGN KEY (FEATURE_SET_ID, FEATURE_SET_VERSION) REFERENCES FEATURESTORE.FEATURE_SET_VERSION(FEATURE_SET_ID, FEATURE_SET_VERSION) ON UPDATE NO ACTION ON DELETE NO ACTION;

CREATE TABLE FEATURESTORE.FEATURE_SET_KEY_TMP (
    FEATURE_SET_ID INTEGER NOT NULL,
    FEATURE_SET_VERSION INTEGER NOT NULL,
    KEY_COLUMN_NAME VARCHAR(128) NOT NULL,
    KEY_COLUMN_DATA_TYPE VARCHAR(128),
    LAST_UPDATE_TS TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    LAST_UPDATE_USERNAME VARCHAR(128) NOT NULL DEFAULT CURRENT_USER,
    PRIMARY KEY(FEATURE_SET_ID, FEATURE_SET_VERSION, KEY_COLUMN_NAME),
    CONSTRAINT fk_feature_set_key_feature_set_version FOREIGN KEY (FEATURE_SET_ID, FEATURE_SET_VERSION) REFERENCES FEATURESTORE.FEATURE_SET_VERSION(FEATURE_SET_ID, FEATURE_SET_VERSION) ON UPDATE NO ACTION ON DELETE NO ACTION
);
INSERT INTO FEATURESTORE.FEATURE_SET_KEY_TMP (feature_set_id, feature_set_version, key_column_name, key_column_data_type, last_update_ts, last_update_username)
SELECT feature_set_id, 1, key_column_name, key_column_data_type, last_update_ts, last_update_username
FROM FEATURESTORE.FEATURE_SET_KEY;
DROP TABLE FEATURESTORE.FEATURE_SET_KEY;
RENAME TABLE FEATURESTORE.FEATURE_SET_KEY_TMP TO FEATURE_SET_KEY;

CREATE TABLE FEATURESTORE.FEATURE_VERSION (
    FEATURE_ID INTEGER,
    FEATURE_SET_ID INTEGER,
    FEATURE_SET_VERSION INTEGER,
    PRIMARY KEY (FEATURE_ID, FEATURE_SET_ID, FEATURE_SET_VERSION),
    CONSTRAINT fk_feature_version_feature FOREIGN KEY (FEATURE_ID) REFERENCES FEATURESTORE.FEATURE(FEATURE_ID) ON UPDATE NO ACTION ON DELETE NO ACTION,
    CONSTRAINT fk_feature_version_feature_set_version FOREIGN KEY (FEATURE_SET_ID, FEATURE_SET_VERSION) REFERENCES FEATURESTORE.FEATURE_SET_VERSION(FEATURE_SET_ID, FEATURE_SET_VERSION) ON UPDATE NO ACTION ON DELETE NO ACTION
);
INSERT INTO FEATURESTORE.FEATURE_VERSION (feature_id, feature_set_id, feature_set_version)
SELECT feature_id, feature_set_id, 1
FROM FEATURESTORE.FEATURE;
ALTER TABLE FEATURESTORE.FEATURE DROP COLUMN FEATURE_SET_ID;
