// // server/server.js
// // Entry point. Starts Express server, loads DB modules, performs WAL recovery, serves frontend.

// const express = require('express');
// const bodyParser = require('body-parser');
// const path = require('path');

// const storage = require('./db/storage');
// const wal = require('./db/wal');
// const parser = require('./db/parser');
// const executor = require('./db/executor');
// const indexer = require('./db/indexer');

// const app = express();
// app.use(bodyParser.json());
// app.use(express.static(path.join(__dirname, '..', 'public')));

// // Ensure data and index folders exist and run recovery
// storage.init();
// wal.recover(); // replay committed ops

// // API: POST /query  -> { sql: 'SELECT ...' }
// app.post('/query', async (req, res) => {
//   const sql = (req.body.sql || '').trim();
//   if (!sql) return res.json({ error: 'Empty query' });

//   const response = {
//     sql,
//     tokens: null,
//     parseTree: null,
//     plan: null,
//     executionSteps: [],
//     result: null,
//     error: null
//   };

//   try {
//     // Tokenize
//     const tokens = parser.tokenize(sql);
//     response.tokens = tokens;

//     // Parse
//     const parseTree = parser.parse(tokens);
//     response.parseTree = parseTree;

//     // If the command is modifying (CREATE/INSERT) we log to WAL first for durability
//     if (parseTree.type === 'CREATE') {
//       // Create table in catalog and persist
//       storage.createTable(parseTree.tableName, parseTree.columns);
//       response.executionSteps.push(`Created table ${parseTree.tableName}`);
//       response.result = [];
//       return res.json(response);
//     }

//     if (parseTree.type === 'INSERT') {
//       // WAL: append INSERT before applying
//       wal.append({ op: 'INSERT', table: parseTree.table, values: parseTree.values });
//       // Apply
//       const step = executor.executeInsert(parseTree.table, parseTree.values);
//       // update index
//       indexer.updateIndexOnInsert(parseTree.table, step.insertedRowId, step.row);
//       response.executionSteps.push(...step.steps);
//       response.result = [];
//       return res.json(response);
//     }

//     if (parseTree.type === 'SELECT') {
//       // Plan: ask executor for plan (string) & run it
//       const plan = executor.planSelect(parseTree);
//       response.plan = plan;
//       response.executionSteps.push(`Plan chosen: ${plan}`);

//       const execResult = executor.executeSelect(parseTree, response.executionSteps);
//       response.result = execResult;
//       return res.json(response);
//     }

//     response.error = 'Unsupported command';
//     return res.json(response);

//   } catch (err) {
//     console.error('Error processing query:', err);
//     response.error = (err && err.message) ? err.message : String(err);
//     return res.json(response);
//   }
// });

// // start server
// const PORT = 3000;
// app.listen(PORT, () => {
//   console.log(`Mini DB server running at http://localhost:${PORT}`);
// });


// // server/server.js
// const express = require('express');
// const bodyParser = require('body-parser');
// const path = require('path');

// const storage = require('./db/storage');
// const wal = require('./db/wal');
// const parser = require('./db/parser');
// const executor = require('./db/executor');
// const indexer = require('./db/indexer');

// const app = express();
// app.use(bodyParser.json());
// app.use(express.static(path.join(__dirname, '..', 'public')));

// // Init storage & recover WAL
// storage.init();
// wal.recover();

// // POST /query
// app.post('/query', async (req, res) => {
//   const sql = (req.body.sql || '').trim();
//   if (!sql) return res.json({ error: 'Empty query' });

//   const response = {
//     sql,
//     tokens: null,
//     parseTree: null,
//     plan: null,
//     executionSteps: [],
//     result: null,
//     error: null
//   };

//   try {
//     // Tokenize & parse
//     const tokens = parser.tokenize(sql);
//     const parseTree = parser.parse(tokens);

//     response.tokens = tokens;
//     response.parseTree = parseTree;

//     // Handle CREATE
//     if (parseTree.type === 'CREATE') {
// wal.append({ op: 'INSERT', table: tableName, values: rowValues }); // ✅ correct

//       storage.createTable(parseTree.tableName, parseTree.columns);
//       response.executionSteps.push(`Created table ${parseTree.tableName}`);
//       response.result = [];
//       return res.json(response);
//     }

//     // Handle INSERT
//     if (parseTree.type === 'INSERT') {
//       const step = executor.executeInsert(parseTree.table, parseTree.values);
//       indexer.updateIndexOnInsert(parseTree.table, step.insertedRowId, step.row);
//       response.executionSteps.push(...step.steps);
//       response.result = [];
//       return res.json(response);
//     }

//     // Handle SELECT
//     if (parseTree.type === 'SELECT') {
//       const plan = executor.planSelect(parseTree);
//       response.plan = plan;
//       response.executionSteps.push(`Plan chosen: ${plan}`);
//       const execResult = executor.executeSelect(parseTree, response.executionSteps);
//       response.result = execResult;
//       return res.json(response);
//     }

//     response.error = 'Unsupported command';
//     return res.json(response);

//   } catch (err) {
//     response.error = err.message || String(err);
//     return res.json(response);
//   }
// });

// // Start server
// const PORT = 3000;
// app.listen(PORT, () => console.log(`Mini DB server running at http://localhost:${PORT}`));


// server/server.js
const express = require('express');
const bodyParser = require('body-parser');
const path = require('path');

const storage = require('./db/storage');
const wal = require('./db/wal');
const parser = require('./db/parser');
const executor = require('./db/executor');
const indexer = require('./db/indexer');

const app = express();
app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, '..', 'public')));

// Initialize storage & recover WAL
storage.init();
wal.recover();

// POST /query
app.post('/query', async (req, res) => {
  const sql = (req.body.sql || '').trim();
  if (!sql) return res.json({ error: 'Empty query' });

  const response = {
    sql,
    tokens: null,
    parseTree: null,
    plan: null,
    executionSteps: [],
    result: null,
    error: null
  };

  try {
    // Tokenize & parse
    const tokens = parser.tokenize(sql);
    const parseTree = parser.parse(tokens);

    response.tokens = tokens;
    response.parseTree = parseTree;

    // Handle CREATE
    if (parseTree.type === 'CREATE') {
      // WAL logging for CREATE
      wal.append({
        op: 'CREATE',
        tableName: parseTree.tableName,
        columns: parseTree.columns
      });

      // Create table in storage
      const schema = storage.getTableSchema(parseTree.tableName);
      if (!schema) {
        storage.createTable(parseTree.tableName, parseTree.columns);
        response.executionSteps.push(`Created table ${parseTree.tableName}`);
      } else {
        response.executionSteps.push(`Table ${parseTree.tableName} already exists, skipping CREATE`);
      }

      response.result = [];
      return res.json(response);
    }

    // Handle INSERT
    if (parseTree.type === 'INSERT') {
      const schema = storage.getTableSchema(parseTree.table);
      if (!schema) throw new Error(`Table ${parseTree.table} does not exist`);

      // Map parsed columns to schema order, fill missing with null
      const rowValues = schema.map(col => {
        const idx = parseTree.columns.indexOf(col);
        return idx >= 0 ? parseTree.values[idx] : null;
      });

      // WAL logging for INSERT
      wal.append({
        op: 'INSERT',
        table: parseTree.table,
        values: rowValues
      });

      // Execute insert
      const step = executor.executeInsert(parseTree.table, rowValues);
      indexer.updateIndexOnInsert(parseTree.table, step.insertedRowId, step.row);
      response.executionSteps.push(...step.steps);
      response.result = [];
      return res.json(response);
    }

    // Handle SELECT
    if (parseTree.type === 'SELECT') {
      const plan = executor.planSelect(parseTree);
      response.plan = plan;
      response.executionSteps.push(`Plan chosen: ${plan}`);
      const execResult = executor.executeSelect(parseTree, response.executionSteps);
      response.result = execResult;
      return res.json(response);
    }

    response.error = 'Unsupported command';
    return res.json(response);

  } catch (err) {
    response.error = err.message || String(err);
    return res.json(response);
  }
});

// Start server
const PORT = 3000;
app.listen(PORT, () => console.log(`Mini DB server running at http://localhost:${PORT}`));
