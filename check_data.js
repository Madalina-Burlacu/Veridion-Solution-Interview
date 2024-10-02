const fs = require('fs');
const csv = require('csv-parser');

let rowCount = 0;
let companies = new Set();

fs.createReadStream('merged_dataset.csv')
    .pipe(csv())
    .on('data', (row) => {
        rowCount++;
        companies.add(row['company_name']); // Înlocuiește cu numele coloanei corespunzătoare

        // Verifică valori lipsă în coloana 'address'
        if (!row['address']) {
            console.log(`Missing address in row ${rowCount}`);
        }
    })
    .on('end', () => {
        console.log(`Total rows: ${rowCount}`);
        console.log(`Unique companies: ${companies.size}`);
    })
    .on('error', (error) => {
        console.error('Error reading CSV file:', error);
    });
