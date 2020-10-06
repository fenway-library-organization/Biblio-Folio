CREATE TABLE exports (
    id           INTEGER PRIMARY KEY,
    mode         VARCHAR NOT NULL DEFAULT 'incremental',
    began        REAL    NOT NULL,
    ended        REAL        NULL,
    status       VARCHAR NOT NULL DEFAULT 'running',
    comment      VARCHAR     NULL,
    num_records  INT DEFAULT 0
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
