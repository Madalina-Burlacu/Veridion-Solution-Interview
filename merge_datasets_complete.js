const fs = require('fs');
const csv = require('csv-parser');
const fastcsv = require('fast-csv');

console.log("Script started...");

// Utility function to clean special characters and normalize text
const cleanText = (text) => {
    return text
        .toLowerCase() // Convert to lowercase
        .replace(/[+,-/()|]/g, '') // Remove special characters
        .replace(/\s+/g, ' ') // Replace multiple spaces with a single space
        .trim(); // Trim leading and trailing spaces
};

// Function to clean company names by removing suffixes like 'Inc.', 'Ltd.', etc.
const cleanCompanyName = (name) => {
    return cleanText(name)
        .replace(/\b(inc|ltd|llc|corp|co)\b/gi, ''); // Remove company suffixes
};

// Function to split categories and clean them
const processCategories = (categories) => {
    return categories
        .split('|')
        .map(category => cleanText(category.trim())); // Split, trim, and clean
};

// Function to concatenate employee information for the Google dataset
const concatenateEmployeeInfo = (row) => {
    let employeeInfo = '';

    // Assuming 'employee' related columns start with 'employee'
    Object.keys(row).forEach((column) => {
        if (column.toLowerCase().includes('employee')) {
            employeeInfo += ` ${row[column]}`; // Concatenate all employee-related fields
        }
    });

    return employeeInfo.trim();
};

// Function to read and process CSV files
const processCSV = (inputFile, datasetName, isGoogle, callback) => {
    const rows = [];

    fs.createReadStream(inputFile)
        .pipe(csv())
        .on('data', (row) => {
            const cleanedRow = {};
            let concatenatedInfo = '';

            for (const column in row) {
                let value = row[column];

                // Clean specific columns
                if (column.toLowerCase().includes('category')) {
                    value = JSON.stringify(processCategories(value));
                } else if (column.toLowerCase().includes('name')) {
                    value = cleanCompanyName(value);
                } else if (column.toLowerCase().includes('address') || 
                           column.toLowerCase().includes('country') || 
                           column.toLowerCase().includes('region')) {
                    value = cleanText(value);
                } else {
                    value = cleanText(value); // General cleaning for all columns
                }

                // Store the cleaned value in the prefixed column name
                cleanedRow[`${datasetName}_${column.toLowerCase()}`] = value;

                // Concatenate all cleaned values to form the 'all_info' string
                concatenatedInfo += `${value} `;
            }

            if (isGoogle) {
                // For the Google dataset, concatenate employee info
                const employeeInfo = concatenateEmployeeInfo(row);
                cleanedRow['google_employee_info'] = employeeInfo;
            }

            // Filter out rows without a domain
            if (cleanedRow[`${datasetName}_domain`] && cleanedRow[`${datasetName}_domain`].trim() !== '') {
                rows.push(cleanedRow);
            }
        })
        .on('end', () => {
            callback(rows);
        });
};

// Function to merge the Google and Facebook datasets
const mergeDatasets = () => {
    processCSV('google_dataset.csv', 'google', true, (googleData) => {
        processCSV('facebook_dataset.csv', 'facebook', false, (facebookData) => {
            const mergedData = [];

            googleData.forEach((googleRow) => {
                const companyName = googleRow['google_name'];

                // Start a new row with Google data
                const mergedRow = { ...googleRow };

                // Check for matching Facebook row
                const matchingFacebookRow = facebookData.find(fbRow => fbRow['facebook_name'] === companyName);
                if (matchingFacebookRow) {
                    // Merge Facebook data into the row
                    for (const key in matchingFacebookRow) {
                        // Only add Facebook columns if they don't exist in the merged row
                        if (!mergedRow.hasOwnProperty(key)) {
                            mergedRow[key] = matchingFacebookRow[key];
                        }
                    }
                }

                mergedData.push(mergedRow);
            });

            // Check for Facebook data not matched with Google
            facebookData.forEach((fbRow) => {
                const companyName = fbRow['facebook_name'];
                const existsInGoogle = googleData.some(googleRow => googleRow['google_name'] === companyName);

                if (!existsInGoogle) {
                    mergedData.push(fbRow); // Add unmatched rows
                }
            });

            // Write the merged data to a new CSV file
            const ws = fs.createWriteStream('merged_dataset.csv');
            fastcsv
                .write(mergedData, { headers: true })
                .pipe(ws)
                .on('finish', () => {
                    console.log('Merged data has been written to merged_dataset.csv');
                });
        });
    });
};

// Start the process
mergeDatasets();
