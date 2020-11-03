CREATE TABLE exports (
    id          INTEGER PRIMARY KEY,
    mode        VARCHAR NOT NULL DEFAULT 'incremental',
    began       REAL    NOT NULL,
    ended       REAL        NULL,
    status      VARCHAR NOT NULL DEFAULT 'running',
    comment     VARCHAR     NULL,
    num_records INT DEFAULT 0
    /*
    ,
    CONSTRAINT CHECK    (ended >= began),
    CONSTRAINT CHECK    (mode IN ('full', 'incremental', 'deletes', 'special')),
    CONSTRAINT CHECK    (status IN ('running', 'completed', 'cancelled', 'failed'))
    */
);
CREATE INDEX exports_index_mode  ON exports(mode);
CREATE INDEX exports_index_began ON exports(began);
CREATE INDEX exports_index_ended ON exports(ended);
CREATE TABLE diagnostics (
    id            INTEGER PRIMARY KEY,
    export        INTEGER NOT NULL,
    record_number INTEGER NOT NULL,
    instance_id   VARCHAR     NULL,
    instance_hrid VARCHAR     NULL,
    code_number   INTEGER NOT NULL,
    warning_msg   VARCHAR     NULL,
    error_msg     VARCHAR     NULL,
    FOREIGN KEY (export) REFERENCES exports(id)
);
CREATE INDEX diagnostics_index_export        ON diagnostics(export);
CREATE INDEX diagnostics_index_record_number ON diagnostics(record_number);
CREATE INDEX diagnostics_index_instance_id   ON diagnostics(instance_id);
CREATE INDEX diagnostics_index_instance_hrid ON diagnostics(instance_hrid);
CREATE INDEX diagnostics_index_code_number   ON diagnostics(code_number);
