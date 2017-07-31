package sql

import (
	"database/sql"
	"fmt"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
	"github.com/lib/pq"
	"github.com/hashicorp/golang-lru"
	"strings"
)

const flavorCockroach = "cockroach"
const nodeCacheSize = 256
var nodeCache *lru.Cache

func init() {
	if nodeCache == nil {
		nodeCache, _ = lru.New(nodeCacheSize)
	}

	RegisterFlavor(Flavor{
		Name:   flavorCockroach,
		Driver: flavorPostgres,
		NodesTable: `CREATE TABLE nodes (
	hash BYTEA PRIMARY KEY,
	value BYTEA,
	value_string TEXT,
	datatype TEXT,
	language TEXT,
	iri BOOLEAN,
	bnode BOOLEAN,
	value_int BIGINT,
	value_bool BOOLEAN,
	value_float double precision,
	value_time timestamp with time zone,
	FAMILY fhash (hash),
	FAMILY fvalue (value, value_string, datatype, language, iri, bnode,
		value_int, value_bool, value_float, value_time)
);`,
		QuadsTable: `CREATE TABLE quads (
	horizon BIGSERIAL PRIMARY KEY,
	subject_hash BYTEA NOT NULL,
	predicate_hash BYTEA NOT NULL,
	object_hash BYTEA NOT NULL,
	label_hash BYTEA,
	id BIGINT,
	ts timestamp
);`,
		FieldQuote:  '"',
		Placeholder: func(n int) string { return fmt.Sprintf("$%d", n) },
		Indexes: func(options graph.Options) []string {
			return []string{
				`CREATE UNIQUE INDEX spol_unique ON quads (subject_hash, predicate_hash, object_hash, label_hash);`,
				`CREATE UNIQUE INDEX spo_unique ON quads (subject_hash, predicate_hash, object_hash);`,
				`CREATE INDEX spo_index ON quads (subject_hash);`,
				`CREATE INDEX pos_index ON quads (predicate_hash);`,
				`CREATE INDEX osp_index ON quads (object_hash);`,
				//`ALTER TABLE quads ADD CONSTRAINT subject_hash_fk FOREIGN KEY (subject_hash) REFERENCES nodes (hash);`,
				//`ALTER TABLE quads ADD CONSTRAINT predicate_hash_fk FOREIGN KEY (predicate_hash) REFERENCES nodes (hash);`,
				//`ALTER TABLE quads ADD CONSTRAINT object_hash_fk FOREIGN KEY (object_hash) REFERENCES nodes (hash);`,
				//`ALTER TABLE quads ADD CONSTRAINT label_hash_fk FOREIGN KEY (label_hash) REFERENCES nodes (hash);`,
			}
		},
		Error: func(err error) error {
			e, ok := err.(*pq.Error)
			if !ok {
				return err
			}
			switch e.Code {
			case "42P07":
				return graph.ErrDatabaseExists
			}
			return err
		},
		//Estimated: func(table string) string{
		//	return "SELECT reltuples::BIGINT AS estimate FROM pg_class WHERE relname='"+table+"';"
		//},
		RunTx:               runTxCockroach,
		NoSchemaChangesInTx: true,
	})
}

// AmbiguousCommitError represents an error that left a transaction in an
// ambiguous state: unclear if it committed or not.
type AmbiguousCommitError struct {
	error
}

// runTxCockroach runs the transaction and will retry in case of a retryable error.
// https://www.cockroachlabs.com/docs/transactions.html#client-side-transaction-retries
func runTxCockroach(tx *sql.Tx, in []graph.Delta, opts graph.IgnoreOpts) error {
	// Specify that we intend to retry this txn in case of CockroachDB retryable
	// errors.
	if _, err := tx.Exec("SAVEPOINT cockroach_restart"); err != nil {
		return err
	}

	for {
		released := false

		err := tryRunTxCockroach(tx, in, opts)

		if err == nil {
			// RELEASE acts like COMMIT in CockroachDB. We use it since it gives us an
			// opportunity to react to retryable errors, whereas tx.Commit() doesn't.
			released = true
			if _, err = tx.Exec("RELEASE SAVEPOINT cockroach_restart"); err == nil {
				return nil
			}
		}
		// We got an error; let's see if it's a retryable one and, if so, restart. We look
		// for either the standard PG errcode SerializationFailureError:40001 or the Cockroach extension
		// errcode RetriableError:CR000. The Cockroach extension has been removed server-side, but support
		// for it has been left here for now to maintain backwards compatibility.
		pqErr, ok := err.(*pq.Error)
		if retryable := ok && (pqErr.Code == "CR000" || pqErr.Code == "40001"); !retryable {
			if released {
				err = &AmbiguousCommitError{err}
			}
			return err
		}
		if _, err = tx.Exec("ROLLBACK TO SAVEPOINT cockroach_restart"); err != nil {
			return err
		}
	}
}

// Processes one Quad value for use in inserts, adding it to the insertValue map if i needs to be written
func processQuadDirection(insertValue map[int][]interface{}, val quad.Value) (hash NodeHash) {

		if val == nil {
			return
		}
		hash = hashOf(val)
		if !hash.Valid() {
			return
		} else if _, ok := nodeCache.Get(hash); ok {
			return
		}
		nodeKey, values, err := nodeValues(hash, val)
		if err != nil {
			panic(err)
		}

		if _, ok := insertValue[nodeKey]; !ok {
			insertValue[nodeKey] = make([]interface{}, 0)
		}
		insertValue[nodeKey] = append(insertValue[nodeKey], values...)
		nodeCache.Add(hash, struct{}{})
	return
}

// tryRunTxCockroach runs the transaction (without retrying).
// For automatic retry upon retryable error use runTxCockroach
func tryRunTxCockroach(tx *sql.Tx, in []graph.Delta, opts graph.IgnoreOpts) (err error) {
	//allAdds := true
	//for _, d := range in {
	//	if d.Action != graph.Add {
	//		allAdds = false
	//	}
	//}
	//if allAdds && !opts.IgnoreDup {
	//	return qs.copyFrom(tx, in, opts)
	//}

	defer func() {
		if r := recover(); r != nil {
				var ok bool
				err, ok = r.(error)
			if !ok {
				err = fmt.Errorf("tx run error: %v", r)
			}
		}
	}()
	end := ";"
	if true || opts.IgnoreDup {
		end = " ON CONFLICT (subject_hash, predicate_hash, object_hash) DO NOTHING"
	}
	end += " RETURNING NOTHING;"

	var (
		insertQuad  *sql.Stmt

		deleteQuad   *sql.Stmt
		deleteTriple *sql.Stmt
	)
	quadValues := make([]interface{}, 0)
	insertValue := make(map[int][]interface{})     // items to be added for each type
	var quadNum uint64

	for _, d := range in {
		switch d.Action {
		case graph.Add:
			quadNum++
			quadValues = append(quadValues,
				processQuadDirection(insertValue, d.Quad.Subject).toSQL(),
				processQuadDirection(insertValue, d.Quad.Predicate).toSQL(),
				processQuadDirection(insertValue, d.Quad.Object).toSQL(),
				processQuadDirection(insertValue, d.Quad.Label).toSQL(),
				d.ID.Int(),
				d.Timestamp,
			)

			if err != nil {
				clog.Errorf("couldn't exec INSERT statement: %v", err)
				return err
			}
		case graph.Delete:
			if deleteQuad == nil {
				deleteQuad, err = tx.Prepare(`DELETE FROM quads WHERE subject_hash=$1 and predicate_hash=$2 and object_hash=$3 and label_hash=$4;`)
				if err != nil {
					return err
				}
				deleteTriple, err = tx.Prepare(`DELETE FROM quads WHERE subject_hash=$1 and predicate_hash=$2 and object_hash=$3 and label_hash is null;`)
				if err != nil {
					return err
				}
			}
			var result sql.Result
			if d.Quad.Label == nil {
				result, err = deleteTriple.Exec(hashOf(d.Quad.Subject).toSQL(), hashOf(d.Quad.Predicate).toSQL(), hashOf(d.Quad.Object).toSQL())
			} else {
				result, err = deleteQuad.Exec(hashOf(d.Quad.Subject).toSQL(), hashOf(d.Quad.Predicate).toSQL(), hashOf(d.Quad.Object).toSQL(), hashOf(d.Quad.Label).toSQL())
			}
			if err != nil {
				clog.Errorf("couldn't exec DELETE statement: %v", err)
				return err
			}
			affected, err := result.RowsAffected()
			if err != nil {
				clog.Errorf("couldn't get DELETE RowsAffected: %v", err)
				return err
			}
			if affected != 1 && !opts.IgnoreMissing {
				return graph.ErrQuadNotExist
			}
		default:
			panic("unknown action")
		}
	}

// node insert
	for nodeKey, valueLists := range insertValue {
		insertStr := `INSERT INTO nodes(hash, ` + strings.Join(nodeInsertColumns[nodeKey], ", ") + ") VALUES "
		valuesNum := uint64(len(nodeInsertColumns[nodeKey])+1)
		for i := uint64(0); ; i++ {
			baseNum := i * valuesNum + 1
			insertStr += fmt.Sprintf("(")
			for j := uint64(0); ; j++ {
				insertStr += fmt.Sprintf("$%d", baseNum+j)

				if j == valuesNum-1 {
					insertStr += fmt.Sprintf(")")
					break
				} else {
					insertStr += ", "
				}

			}
			if i == uint64(len(valueLists))/(valuesNum)-1 {
				break
			} else {
				insertStr += ", "
			}
		}
		insertStr += " ON CONFLICT (hash) DO NOTHING RETURNING NOTHING;"

		insertItem, err := tx.Prepare(insertStr)
		if err != nil {
			return err
		}
		_, err = insertItem.Exec(valueLists...)
		if err != nil {
			return err
		}
	}


	// quad insert
	if quadNum > 0 {
		insertQuadStr := `INSERT INTO quads(subject_hash, predicate_hash, object_hash, label_hash, id, ts) VALUES `
		for i := uint64(0); ; i++ {
			baseNum := i*6
			insertQuadStr += fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)", baseNum+1, baseNum+2, baseNum+3, baseNum+4, baseNum+5, baseNum+6)
			if i == quadNum-1 {
				break
			} else {
				insertQuadStr += ", "
			}
		}
		insertQuadStr += end
		insertQuad, err = tx.Prepare(insertQuadStr)
		if err != nil {
			fmt.Println("111")
			return err
		}
		_, err = insertQuad.Exec(quadValues...)
		if err != nil {
			fmt.Println("111")
			return err
		}

		// fmt.Printf("%d -- %d -- %s\n", len(in), quadNum, insertQuadStr)

	}

	return nil
}
