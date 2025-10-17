// // server/db/parser.js
// // Tokenizer + simple parser supporting:
// //  - CREATE TABLE <name> (col1, col2, ...)
// //  - INSERT INTO <name> VALUES (v1, v2, ...)
// //  - SELECT <cols|*> FROM <name> [WHERE col op value]
// //
// // returns parse trees used by executor.

// function tokenize(sql) {
//   // simple tokenizer, returns tokens preserving case for identifiers/values
//   // tokens: words, numbers, operators, parentheses, commas, asterisks, semicolon
//   const re = /\s*(\*|<=|>=|!=|=|<|>|\(|\)|,|;|\'[^\']*\'|"[^"]*"|[A-Za-z_][A-Za-z0-9_]*|\d+)\s*/g;
//   const tokens = [];
//   let m;
//   while ((m = re.exec(sql)) !== null) {
//     let tok = m[1];
//     // strip quotes from string literals but keep them as values
//     if ((tok.startsWith("'") && tok.endsWith("'")) || (tok.startsWith('"') && tok.endsWith('"'))) {
//       tok = tok.slice(1, -1);
//     }
//     tokens.push(tok);
//   }
//   return tokens;
// }

// function parse(tokens) {
//   if (!tokens || tokens.length === 0) throw new Error('Empty query');
//   const upper0 = String(tokens[0]).toUpperCase();

//   // CREATE TABLE name (col, col)
//   if (upper0 === 'CREATE' && String(tokens[1] || '').toUpperCase() === 'TABLE') {
//     const tableName = tokens[2];
//     // gather columns inside parentheses
//     const start = tokens.indexOf('(');
//     const end = tokens.indexOf(')');
//     if (start === -1 || end === -1 || end <= start) throw new Error('Invalid CREATE TABLE syntax');
//     const cols = tokens.slice(start + 1, end).filter(t => t !== ',');
//     return { type: 'CREATE', tableName, columns: cols };
//   }

//   // INSERT INTO name VALUES (v1, v2)
//   if (upper0 === 'INSERT' && String(tokens[1] || '').toUpperCase() === 'INTO') {
//     const table = tokens[2];
//     const valuesIndex = tokens.findIndex(t => String(t).toUpperCase() === 'VALUES');
//     if (valuesIndex === -1) throw new Error('Invalid INSERT syntax');
//     const start = tokens.indexOf('(');
//     const end = tokens.indexOf(')');
//     const vals = tokens.slice(start + 1, end).filter(t => t !== ',').map(v => {
//       // try number
//       if (!isNaN(v)) return Number(v);
//       return v;
//     });
//     return { type: 'INSERT', table, values: vals };
//   }

//   // SELECT col1, col2 FROM table [WHERE col op val]
//   if (upper0 === 'SELECT') {
//     const fromIdx = tokens.findIndex(t => String(t).toUpperCase() === 'FROM');
//     if (fromIdx === -1) throw new Error('Invalid SELECT: missing FROM');
//     const cols = tokens.slice(1, fromIdx).filter(t => t !== ',');
//     const table = tokens[fromIdx + 1];
//     let where = null;
//     const whereIdx = tokens.findIndex(t => String(t).toUpperCase() === 'WHERE');
//     if (whereIdx !== -1) {
//       const column = tokens[whereIdx + 1];
//       const op = tokens[whereIdx + 2];
//       const valRaw = tokens[whereIdx + 3];
//       const val = (valRaw && !isNaN(valRaw)) ? Number(valRaw) : valRaw;
//       where = { column, op, value: val };
//     }
//     return { type: 'SELECT', table, columns: cols, where };
//   }

//   throw new Error('Unsupported or invalid SQL command');
// }

// module.exports = { tokenize, parse };
// server/db/parser.js
// Tokenizer + simple parser supporting CREATE, INSERT, SELECT

function tokenize(sql) {
  const re = /\s*(\*|<=|>=|!=|=|<|>|\(|\)|,|;|\'[^\']*\'|"[^"]*"|[A-Za-z_][A-Za-z0-9_]*|\d+)\s*/g;
  const tokens = [];
  let m;
  while ((m = re.exec(sql)) !== null) {
    let tok = m[1];
    if ((tok.startsWith("'") && tok.endsWith("'")) || (tok.startsWith('"') && tok.endsWith('"'))) {
      tok = tok.slice(1, -1);
    }
    tokens.push(tok);
  }
  return tokens;
}

function parse(tokens) {
  if (!tokens || tokens.length === 0) throw new Error('Empty query');
  const upper0 = String(tokens[0]).toUpperCase();

  if (upper0 === 'CREATE' && String(tokens[1] || '').toUpperCase() === 'TABLE') {
    const tableName = tokens[2];
    const start = tokens.indexOf('(');
    const end = tokens.indexOf(')');
    if (start === -1 || end === -1 || end <= start) throw new Error('Invalid CREATE TABLE syntax');
    const cols = tokens.slice(start + 1, end).filter(t => t !== ',');
    return { type: 'CREATE', tableName, columns: cols };
  }

  if (upper0 === 'INSERT' && String(tokens[1] || '').toUpperCase() === 'INTO') {
    const table = tokens[2];
    const valuesIndex = tokens.findIndex(t => String(t).toUpperCase() === 'VALUES');
    if (valuesIndex === -1) throw new Error('Invalid INSERT syntax');
    const start = tokens.indexOf('(');
    const end = tokens.indexOf(')');
    const vals = tokens.slice(start + 1, end).filter(t => t !== ',').map(v => (!isNaN(v) ? Number(v) : v));
    return { type: 'INSERT', table, values: vals };
  }

  if (upper0 === 'SELECT') {
    const fromIdx = tokens.findIndex(t => String(t).toUpperCase() === 'FROM');
    if (fromIdx === -1) throw new Error('Invalid SELECT: missing FROM');
    const cols = tokens.slice(1, fromIdx).filter(t => t !== ',');
    const table = tokens[fromIdx + 1];
    let where = null;
    const whereIdx = tokens.findIndex(t => String(t).toUpperCase() === 'WHERE');
    if (whereIdx !== -1) {
      const column = tokens[whereIdx + 1];
      const op = tokens[whereIdx + 2];
      const valRaw = tokens[whereIdx + 3];
      const val = valRaw && !isNaN(valRaw) ? Number(valRaw) : valRaw;
      where = { column, op, value: val };
    }
    return { type: 'SELECT', table, columns: cols, where };
  }

  throw new Error('Unsupported or invalid SQL command');
}

module.exports = { tokenize, parse };
