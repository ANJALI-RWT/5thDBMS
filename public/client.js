// public/client.js
// Handles sending SQL to backend and updating visualization panels.

async function postSQL(sql) {
  try {
    const res = await fetch('/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql })
    });
    return await res.json();
  } catch (err) {
    return { error: 'Server unreachable' };
  }
}

document.getElementById('runBtn').addEventListener('click', async () => {
  const sql = document.getElementById('sqlInput').value;
  if (!sql.trim()) return alert('Type a query first');

  // Clear previous results/panels
  ['tokens', 'parseTree', 'plan', 'steps', 'resultArea'].forEach(id => {
    document.getElementById(id).textContent = '';
  });
  document.getElementById('resultArea').innerHTML = '';

  const resp = await postSQL(sql);

  // Display tokens, parse tree, plan, and execution steps
  document.getElementById('tokens').textContent = JSON.stringify(resp.tokens, null, 2);
  document.getElementById('parseTree').textContent = JSON.stringify(resp.parseTree, null, 2);
  document.getElementById('plan').textContent = resp.plan || '';
  document.getElementById('steps').textContent = (resp.executionSteps || []).join('\n');

  const area = document.getElementById('resultArea');

  if (resp.error) {
    area.innerHTML = `<div style="color:red">Error: ${resp.error}</div>`;
    return;
  }

  if (!resp.result || resp.result.length === 0) {
    area.innerHTML = `<div>No rows</div>`;
    return;
  }

  // Build table robustly: handle missing keys in some rows
  const keys = Array.from(new Set(resp.result.flatMap(r => Object.keys(r))));
  let html = '<table border="1"><thead><tr>' +
             keys.map(k => `<th>${k}</th>`).join('') +
             '</tr></thead><tbody>';

  resp.result.forEach(r => {
    html += '<tr>' + keys.map(k => `<td>${r[k] !== undefined ? r[k] : ''}</td>`).join('') + '</tr>';
  });

  html += '</tbody></table>';
  area.innerHTML = html;
});

document.getElementById('recoverBtn').addEventListener('click', async () => {
  alert('Recovery happens automatically on server start. Restart the server to replay WAL.');
});
