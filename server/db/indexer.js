// // server/db/indexer.js
// // Very simple hash-index stored on disk: indexes/<table>__<col>.json
// // Maps value -> [rowIndices]. (Row indices are positions in the data array)
// //
// // For simplicity, index is an optional feature and built/updated on insert.

// const fs = require('fs');
// const path = require('path');
// const storage = require('./storage');

// const INDEX_DIR = path.join(__dirname, '..', 'indexes');
// if (!fs.existsSync(INDEX_DIR)) fs.mkdirSync(INDEX_DIR);

// function indexFilePath(table, column) {
//   return path.join(INDEX_DIR, `${table}__${column}.json`);
// }

// function buildIndex(table, column) {
//   const rows = storage.readTable(table);
//   const idx = {};
//   rows.forEach((r, i) => {
//     const key = r[column];
//     if (!idx[key]) idx[key] = [];
//     idx[key].push(i);
//   });
//   fs.writeFileSync(indexFilePath(table, column), JSON.stringify(idx, null, 2));
//   return idx;
// }

// function getIndex(table, column) {
//   const file = indexFilePath(table, column);
//   if (!fs.existsSync(file)) return null;
//   return JSON.parse(fs.readFileSync(file, 'utf8'));
// }

// function updateIndexOnInsert(table, rowIndex, row) {
//   // update all indexes present for this table
//   const files = fs.readdirSync(INDEX_DIR);
//   files.filter(f => f.startsWith(`${table}__`)).forEach(f => {
//     const column = f.split('__')[1].replace('.json','');
//     const idxPath = path.join(INDEX_DIR, f);
//     let idx = JSON.parse(fs.readFileSync(idxPath,'utf8'));
//     const key = row[column];
//     if (!idx[key]) idx[key] = [];
//     idx[key].push(rowIndex);
//     fs.writeFileSync(idxPath, JSON.stringify(idx, null, 2));
//   });
// }

// function lookup(table, column, value) {
//   const idx = getIndex(table, column);
//   if (!idx) return null;
//   return idx[value] || null;
// }

// module.exports = { buildIndex, getIndex, updateIndexOnInsert, lookup };

// server/db/indexer.js
// Simple hash-index stored on disk: indexes/<table>__<col>.json

const fs = require('fs');
const path = require('path');
const storage = require('./storage');

const INDEX_DIR = path.join(__dirname, '..', 'indexes');
if (!fs.existsSync(INDEX_DIR)) fs.mkdirSync(INDEX_DIR);

function indexFilePath(table, column) {
  return path.join(INDEX_DIR, `${table}__${column}.json`);
}

function buildIndex(table, column) {
  const rows = storage.readTable(table);
  const idx = {};
  rows.forEach((r, i) => {
    const key = String(r[column]);
    if (!idx[key]) idx[key] = [];
    idx[key].push(i);
  });
  fs.writeFileSync(indexFilePath(table, column), JSON.stringify(idx, null, 2));
  return idx;
}

function getIndex(table, column) {
  const file = indexFilePath(table, column);
  if (!fs.existsSync(file)) return null;
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

function updateIndexOnInsert(table, rowIndex, row) {
  const files = fs.readdirSync(INDEX_DIR);
  files.filter(f => f.startsWith(`${table}__`)).forEach(f => {
    const column = f.split('__')[1].replace('.json', '');
    const idxPath = path.join(INDEX_DIR, f);
    let idx = JSON.parse(fs.readFileSync(idxPath, 'utf8'));
    const key = String(row[column] || '');
    if (!idx[key]) idx[key] = [];
    idx[key].push(rowIndex);
    fs.writeFileSync(idxPath, JSON.stringify(idx, null, 2));
  });
}

function lookup(table, column, value) {
  const idx = getIndex(table, column);
  if (!idx) return null;
  return idx[String(value)] || null;
}

module.exports = { buildIndex, getIndex, updateIndexOnInsert, lookup };
