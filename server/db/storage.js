// // server/db/storage.js
// // Simple file-based table storage + catalog for schemas.
// // Tables persisted as JSON arrays in server/data/<Table>.json
// // Catalog stored in server/db/catalog.json

// const fs = require('fs');
// const path = require('path');

// const DATA_DIR = path.join(__dirname, '..', 'data');
// const CATALOG = path.join(__dirname, 'catalog.json');

// // ensure folders
// function init() {
//   if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);
//   if (!fs.existsSync(CATALOG)) fs.writeFileSync(CATALOG, JSON.stringify({}, null, 2));
// }

// function loadCatalog() {
//   init();
//   const txt = fs.readFileSync(CATALOG, 'utf8');
//   return JSON.parse(txt || '{}');
// }

// function saveCatalog(cat) {
//   fs.writeFileSync(CATALOG, JSON.stringify(cat, null, 2));
// }

// function createTable(tableName, columns) {
//   init();
//   const cat = loadCatalog();
//   if (cat[tableName]) throw new Error(`Table ${tableName} already exists`);
//   cat[tableName] = { columns }; // keep it simple
//   saveCatalog(cat);

//   const file = path.join(DATA_DIR, `${tableName}.json`);
//   fs.writeFileSync(file, JSON.stringify([]));
//   return true;
// }

// function readTable(tableName) {
//   init();
//   const file = path.join(DATA_DIR, `${tableName}.json`);
//   if (!fs.existsSync(file)) throw new Error(`Table ${tableName} does not exist`);
//   const txt = fs.readFileSync(file, 'utf8');
//   return JSON.parse(txt || '[]');
// }

// function writeTable(tableName, rows) {
//   init();
//   const file = path.join(DATA_DIR, `${tableName}.json`);
//   fs.writeFileSync(file, JSON.stringify(rows, null, 2));
// }

// // Insert returns new row id (index)
// function insertRow(tableName, rowObj) {
//   const rows = readTable(tableName);
//   rows.push(rowObj);
//   writeTable(tableName, rows);
//   return rows.length - 1;
// }

// function getTableSchema(tableName) {
//   const cat = loadCatalog();
//   return cat[tableName] || null;
// }

// module.exports = {
//   init,
//   createTable,
//   readTable,
//   writeTable,
//   insertRow,
//   loadCatalog,
//   getTableSchema
// };


// server/db/storage.js
// Simple file-based table storage + catalog for schemas.

const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, '..', 'data');
const CATALOG = path.join(__dirname, 'catalog.json');

// ensure folders
function init() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);
  if (!fs.existsSync(CATALOG)) fs.writeFileSync(CATALOG, JSON.stringify({}, null, 2));
}

function loadCatalog() {
  init();
  const txt = fs.readFileSync(CATALOG, 'utf8');
  return JSON.parse(txt || '{}');
}

function saveCatalog(cat) {
  fs.writeFileSync(CATALOG, JSON.stringify(cat, null, 2));
}

function createTable(tableName, columns) {
  init();
  const cat = loadCatalog();
  if (cat[tableName]) throw new Error(`Table ${tableName} already exists`);
  cat[tableName] = { columns };
  saveCatalog(cat);

  const file = path.join(DATA_DIR, `${tableName}.json`);
  fs.writeFileSync(file, JSON.stringify([]));
  return true;
}

function readTable(tableName) {
  init();
  const file = path.join(DATA_DIR, `${tableName}.json`);
  if (!fs.existsSync(file)) throw new Error(`Table ${tableName} does not exist`);
  const txt = fs.readFileSync(file, 'utf8');
  return JSON.parse(txt || '[]');
}

function writeTable(tableName, rows) {
  init();
  const file = path.join(DATA_DIR, `${tableName}.json`);
  fs.writeFileSync(file, JSON.stringify(rows, null, 2));
}

// Insert returns new row id (index)
function insertRow(tableName, rowObj) {
  const rows = readTable(tableName);
  rows.push(rowObj);
  writeTable(tableName, rows);
  return rows.length - 1;
}

function getTableSchema(tableName) {
  const cat = loadCatalog();
  return cat[tableName] || null;
}

module.exports = {
  init,
  createTable,
  readTable,
  writeTable,
  insertRow,
  loadCatalog,
  getTableSchema
};
