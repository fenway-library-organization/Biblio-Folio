CREATE TABLE instances (
    id                  VARCHAR UNIQUE PRIMARY KEY,
    hrid                VARCHAR UNIQUE NOT NULL,
    source_type         VARCHAR     NULL,
    source              VARCHAR     NULL,
    last_modified       REAL    NOT NULL,
    suppressed          INTEGER NOT NULL DEFAULT 0,
    deleted             INTEGER NOT NULL DEFAULT 0,
    update_id           INTEGER,
    /*
    CONSTRAINT CHECK    (suppressed IN (0, 1)),
    CONSTRAINT CHECK    (deleted IN (0, 1)),
    */
    FOREIGN KEY         (update_id) REFERENCES updates(id)
);
CREATE TABLE holdings (
    instance_id         VARCHAR NOT NULL,
    holdings_record_id  VARCHAR NOT NULL,
    suppressed          INTEGER DEFAULT 0,
    deleted             INTEGER DEFAULT 0,
    /*
    CONSTRAINT CHECK    (deleted IN (0, 1)),
    */
    FOREIGN KEY         (instance_id) REFERENCES instances(id)
);
CREATE TABLE updates (
    id                  INTEGER PRIMARY KEY,
    type                VARCHAR NOT NULL DEFAULT 'incremental',
    status              VARCHAR NOT NULL DEFAULT 'starting',
    query               VARCHAR     NULL,
    comment             VARCHAR     NULL,
    began               REAL    NOT NULL,
    ended               REAL        NULL,
    after               REAL        NULL,
    before              REAL        NULL,
    max_last_modified   REAL        NULL,
    num_records         INTEGER NOT NULL DEFAULT 0,
    num_errors          INTEGER NOT NULL DEFAULT 0
    /*
    ,
    CONSTRAINT CHECK    (ended >= began),
    CONSTRAINT CHECK    (type IN ('full', 'incremental', 'one-time')),
    CONSTRAINT CHECK    (status IN ('starting', 'running', 'partial', 'completed', 'failed'))
    */
);
/* Indexes on instances */
CREATE INDEX instances_hrid_index              ON instances (hrid);
CREATE INDEX instances_hrid_length_index       ON instances (length(hrid));
CREATE INDEX instances_source_type_index       ON instances (source_type);
CREATE INDEX instances_last_modified_index     ON instances (last_modified);
CREATE INDEX instances_suppressed_index        ON instances (suppressed);
CREATE INDEX instances_deleted_index           ON instances (deleted);
CREATE INDEX instances_update_id_index         ON instances (update_id);
/* Indexes on instance holdings */
CREATE INDEX holdings_instance_id_index        ON holdings (instance_id);
CREATE INDEX holdings_holdings_record_id_index ON holdings (holdings_record_id);
/* Indexes on updates */
CREATE INDEX updates_type_index                ON updates (type);
CREATE INDEX updates_status_index              ON updates (status);
CREATE INDEX updates_began_index               ON updates (began);
CREATE INDEX updates_max_last_modified_index   ON updates (max_last_modified);
