// // server/db/executor.js
// // Execution and basic planner/optimizer.
// // For SELECT: if WHERE.column has an index, use index scan (via indexer.lookup) otherwise full scan.

// const storage = require('./storage');
// const indexer = require('./indexer');

// function executeInsert(table, values) {
//   const schema = storage.getTableSchema(table);
//   if (!schema) throw new Error(`Table ${table} not found`);

//   const columns = schema.columns;
//   if (values.length !== columns.length) {
//     throw new Error(`Column count mismatch for table ${table}`);
//   }

//   const row = {};
//   for (let i = 0; i < columns.length; i++) {
//     row[columns[i]] = values[i];
//   }

//   // WAL append removed here because server.js already handles it

//   const rowId = storage.insertRow(table, row);
//   return { insertedRowId: rowId, row, steps: [`Inserted row at index ${rowId}`] };
// }

// function planSelect(parseTree) {
//   if (parseTree.where && parseTree.where.column) {
//     const idx = indexer.getIndex(parseTree.table, parseTree.where.column);
//     if (idx) {
//       return `IndexScan on ${parseTree.table}.${parseTree.where.column}`;
//     }
//   }
//   return `FullTableScan on ${parseTree.table}`;
// }

// function executeSelect(parseTree, executionSteps) {
//   const table = parseTree.table;
//   const schema = storage.getTableSchema(table);
//   if (!schema) throw new Error(`Table ${table} not found`);

//   const rows = storage.readTable(table);
//   let candidateRows = [];

//   if (parseTree.where && parseTree.where.column) {
//     const idxCandidates = indexer.lookup(table, parseTree.where.column, parseTree.where.value);
//     if (idxCandidates && idxCandidates.length > 0) {
//       executionSteps.push(`Index lookup found ${idxCandidates.length} candidate row(s)`);
//       idxCandidates.forEach(i => candidateRows.push({ row: rows[i], rowIndex: i }));
//     } else {
//       executionSteps.push('No index found — falling back to full table scan');
//       rows.forEach((r, i) => candidateRows.push({ row: r, rowIndex: i }));
//     }
//   } else {
//     executionSteps.push('No WHERE clause — full table scan');
//     rows.forEach((r, i) => candidateRows.push({ row: r, rowIndex: i }));
//   }

//   // Apply filter
//   const filtered = [];
//   for (const { row, rowIndex } of candidateRows) {
//     executionSteps.push(`Reading row ${rowIndex}: ${JSON.stringify(row)}`);
//     let keep = true;

//     if (parseTree.where && parseTree.where.column) {
//       const { column, op, value } = parseTree.where;
//       const rv = row[column];
//       if (op === '=') keep = (rv == value);
//       else if (op === '>') keep = (rv > value);
//       else if (op === '<') keep = (rv < value);
//       else keep = false;

//       executionSteps.push(`  -> Condition ${column} ${op} ${value} ? ${keep}`);
//     }

//     if (keep) filtered.push(row);
//   }

//   // Projection
//   const cols = parseTree.columns;
//   let result;
//   if (cols.length === 1 && cols[0] === '*') {
//     result = filtered;
//   } else {
//     result = filtered.map(r => {
//       const out = {};
//       cols.forEach(c => {
//         if (!(c in r)) console.warn(`Column ${c} not in table ${table}`);
//         out[c] = r[c];
//       });
//       return out;
//     });
//   }

//   executionSteps.push(`Result count: ${result.length}`);
//   return result;
// }

// module.exports = { executeInsert, planSelect, executeSelect };

// server/db/executor.js
// Execution and basic planner/optimizer.

const storage = require('./storage');
const indexer = require('./indexer');

function executeInsert(table, values) {
  const schema = storage.getTableSchema(table);
  if (!schema) throw new Error(`Table ${table} not found`);

  const columns = schema.columns;
  if (values.length !== columns.length) {
    throw new Error(`Column count mismatch for table ${table}`);
  }

  const row = {};
  for (let i = 0; i < columns.length; i++) row[columns[i]] = values[i];

  const rowId = storage.insertRow(table, row);
  return { insertedRowId: rowId, row, steps: [`Inserted row at index ${rowId}`] };
}

function planSelect(parseTree) {
  if (parseTree.where && parseTree.where.column) {
    const idx = indexer.getIndex(parseTree.table, parseTree.where.column);
    if (idx) return `IndexScan on ${parseTree.table}.${parseTree.where.column}`;
  }
  return `FullTableScan on ${parseTree.table}`;
}

function executeSelect(parseTree, executionSteps) {
  const table = parseTree.table;
  const schema = storage.getTableSchema(table);
  if (!schema) throw new Error(`Table ${table} not found`);

  const rows = storage.readTable(table);
  let candidateRows = [];

  if (parseTree.where && parseTree.where.column) {
    const idxCandidates = indexer.lookup(table, parseTree.where.column, parseTree.where.value);
    if (idxCandidates && idxCandidates.length > 0) {
      executionSteps.push(`Index lookup found ${idxCandidates.length} candidate row(s)`);
      idxCandidates.forEach(i => candidateRows.push({ row: rows[i], rowIndex: i }));
    } else {
      executionSteps.push('No index found — falling back to full table scan');
      rows.forEach((r, i) => candidateRows.push({ row: r, rowIndex: i }));
    }
  } else {
    executionSteps.push('No WHERE clause — full table scan');
    rows.forEach((r, i) => candidateRows.push({ row: r, rowIndex: i }));
  }

  // apply filter
  const filtered = [];
  for (const { row, rowIndex } of candidateRows) {
    executionSteps.push(`Reading row ${rowIndex}: ${JSON.stringify(row)}`);
    let keep = true;

    if (parseTree.where && parseTree.where.column) {
      const { column, op, value } = parseTree.where;
      const rv = row[column];
      if (op === '=') keep = (rv == value);
      else if (op === '>') keep = (rv > value);
      else if (op === '<') keep = (rv < value);
      else keep = false;

      executionSteps.push(`  -> Condition ${column} ${op} ${value} ? ${keep}`);
    }

    if (keep) filtered.push(row);
  }

  // projection
  const cols = parseTree.columns;
  let result;
  if (cols.length === 1 && cols[0] === '*') {
    result = filtered;
  } else {
    result = filtered.map(r => {
      const out = {};
      cols.forEach(c => {
        if (!(c in r)) console.warn(`Column ${c} not in table ${table}`);
        out[c] = r[c];
      });
      return out;
    });
  }

  executionSteps.push(`Result count: ${result.length}`);
  return result;
}

module.exports = { executeInsert, planSelect, executeSelect };
