// // // server/db/wal.js
// // // Very small Write-Ahead Log (WAL). Appends JSON lines to wal.log.
// // // On recovery we re-apply INSERTs (simple approach).

// // const fs = require('fs');
// // const path = require('path');
// // const storage = require('./storage');

// // const WAL_FILE = path.join(__dirname, '..', 'wal.log');

// // function append(record) {
// //   const line = JSON.stringify({ ts: Date.now(), record }) + '\n';
// //   fs.appendFileSync(WAL_FILE, line);
// // }

// // // Recover by applying any INSERT ops (idempotent for this demo)
// // function recover() {
// //   if (!fs.existsSync(WAL_FILE)) return;
// //   const lines = fs.readFileSync(WAL_FILE, 'utf8').split('\n').filter(Boolean);
// //   for (const l of lines) {
// //     try {
// //       const parsed = JSON.parse(l);
// //       const rec = parsed.record;
// //       if (rec && rec.op === 'INSERT') {
// //         // Apply insert if table exists
// //         try {
// //           const schema = storage.getTableSchema(rec.table);
// //           if (schema) {
// //             // Append row directly (no WAL logging here to avoid recursion)
// //            storage.insertRow(rec.table, rec.values);

// //             console.log(`WAL recovery applied INSERT into ${rec.table}`);
// //           }
// //         } catch (e) {
// //           console.warn('WAL recovery warning:', e.message);
// //         }
// //       }
// //     } catch (e) {
// //       console.warn('Skipping bad WAL line');
// //     }
// //   }
// // }

// // module.exports = { append, recover };
// // server/db/wal.js
// // Simple Write-Ahead Log (WAL) for CREATE and INSERT operations.

// const fs = require('fs');
// const path = require('path');
// const storage = require('./storage');

// const WAL_FILE = path.join(__dirname, '..', 'wal.log');

// // Append a record to WAL
// function append(record) {
//   const line = JSON.stringify({ ts: Date.now(), record }) + '\n';
//   fs.appendFileSync(WAL_FILE, line); // synchronous for simplicity
// }

// // Recover by applying CREATE and INSERT ops
// function recover() {
//   if (!fs.existsSync(WAL_FILE)) return;

//   const lines = fs.readFileSync(WAL_FILE, 'utf8')
//     .split('\n')
//     .filter(Boolean);

//   for (const l of lines) {
//     try {
//       const parsed = JSON.parse(l);
//       const rec = parsed.record;
//       if (!rec) continue;

//       if (rec.op === 'CREATE') {
//         storage.createTable(rec.tableName, rec.columns);
//         console.log(`WAL recovery applied CREATE table ${rec.tableName}`);
//       } else if (rec.op === 'INSERT') {
//         const schema = storage.getTableSchema(rec.table);
//         if (schema) {
//           storage.insertRow(rec.table, rec.values);
//           console.log(`WAL recovery applied INSERT into ${rec.table}`);
//         }
//       }
//     } catch (e) {
//       console.warn('Skipping bad WAL line:', l);
//     }
//   }
// }

// module.exports = { append, recover };




const fs = require('fs');
const path = require('path');
const storage = require('./storage');

const WAL_FILE = path.join(__dirname, '..', 'wal.log');



function append(record) {
  const line = JSON.stringify({ ts: Date.now(), record }) + '\n';
  fs.appendFileSync(WAL_FILE, line); 
  
}



function recover() {
  if (!fs.existsSync(WAL_FILE)) return;


  const lines = fs.readFileSync(WAL_FILE, 'utf8')
    .split('\n')
    .filter(Boolean);
    

  for (const l of lines) {
    try {
      const parsed = JSON.parse(l);
      const rec = parsed.record;
      if (!rec) continue;

     
      

      if (rec.op === 'CREATE') {
        const schema = storage.getTableSchema(rec.tableName);
        if (!schema) { 
            
          storage.createTable(rec.tableName, rec.columns);
          console.log(`WAL recovery applied CREATE table ${rec.tableName}`);
        } else {
          console.log(`Table ${rec.tableName} already exists, skipping CREATE`);
        }

        

      } else if (rec.op === 'INSERT') {
        const schema = storage.getTableSchema(rec.table);
        if (schema) {
          
            

          const row = schema.map(col => rec.values[col] !== undefined ? rec.values[col] : null);
          storage.insertRow(rec.table, row);
          console.log(`WAL recovery applied INSERT into ${rec.table}`);
        }
      }

    } catch (e) {
      console.warn('Skipping bad WAL line:', l);
    }
  }
}


module.exports = { append, recover };

