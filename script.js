// ======= Simulated DB =======
let database = {
    Students: [
        {ID: 1, Name: 'Anjali', Age: 20},
        {ID: 2, Name: 'Rahul', Age: 21},
        {ID: 3, Name: 'Simran', Age: 19}
    ]
};

// ======= Event Listener =======
document.getElementById('executeBtn').addEventListener('click', () => {
    let query = document.getElementById('queryInput').value;
    simulateDBMS(query);
});

// ======= DBMS Simulation =======
function simulateDBMS(query) {
    // 1️⃣ Tokenization
    const tokens = tokenize(query);
    document.getElementById('tokens').textContent = JSON.stringify(tokens, null, 2);

    // 2️⃣ Parsing
    const parseTree = parseQuery(tokens);
    document.getElementById('parseTree').textContent = JSON.stringify(parseTree, null, 2);

    // 3️⃣ Execution Plan
    const plan = generatePlan(parseTree);
    document.getElementById('executionPlan').textContent = plan;

    // 4️⃣ Execution
    const result = executeQuery(parseTree);
    document.getElementById('queryExecution').textContent = JSON.stringify(result, null, 2);

    // 5️⃣ Display Result Table
    displayResult(result);
}

// ======= Tokenizer =======
function tokenize(query) {
    return query.match(/\w+|\*|=|>|<|,|\(|\)|;/g) || [];
}

// ======= Parser =======
function parseQuery(tokens) {
    if(tokens.length === 0) return {};
    const command = tokens[0].toUpperCase();
    if(command === "SELECT") {
        let columns = [];
        let table = "";
        let where = null;

        let i = 1;
        if(tokens[i] === "*") { columns.push("*"); i++; }
        else {
            while(tokens[i] && tokens[i] !== "FROM") {
                if(tokens[i] !== ",") columns.push(tokens[i]);
                i++;
            }
        }

        if(tokens[i] && tokens[i].toUpperCase() === "FROM") i++;
        table = tokens[i];
        i++;

        if(tokens[i] && tokens[i].toUpperCase() === "WHERE") {
            i++;
            where = {column: tokens[i], operator: tokens[i+1], value: tokens[i+2]};
        }

        return {command, columns, table, where};
    }

    return {command: "UNKNOWN"};
}

// ======= Execution Plan =======
function generatePlan(parseTree) {
    if(parseTree.command === "SELECT") {
        return `Plan: Scan table "${parseTree.table}"` + (parseTree.where ? `, Apply filter on "${parseTree.where.column}"` : '');
    }
    return "No plan available";
}

// ======= Execution =======
function executeQuery(parseTree) {
    if(parseTree.command !== "SELECT") return [];

    let rows = database[parseTree.table] || [];
    if(parseTree.where) {
        const {column, operator, value} = parseTree.where;
        rows = rows.filter(row => {
            if(operator === "=") return row[column] == value;
            if(operator === ">") return row[column] > value;
            if(operator === "<") return row[column] < value;
            return false;
        });
    }

    if(parseTree.columns.includes("*")) return rows;
    else return rows.map(row => {
        let filtered = {};
        parseTree.columns.forEach(col => filtered[col] = row[col]);
        return filtered;
    });
}

// ======= Display Result =======
function displayResult(result) {
    const head = document.getElementById('resultHead');
    const body = document.getElementById('resultBody');
    head.innerHTML = '';
    body.innerHTML = '';

    if(result.length === 0) return;

    // Table Headers
    const headers = Object.keys(result[0]);
    head.innerHTML = '<tr>' + headers.map(h => `<th>${h}</th>`).join('') + '</tr>';

    // Table Rows
    result.forEach(row => {
        const tr = document.createElement('tr');
        headers.forEach(h => {
            const td = document.createElement('td');
            td.textContent = row[h];
            tr.appendChild(td);
        });
        body.appendChild(tr);
    });
}
