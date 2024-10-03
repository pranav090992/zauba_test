
let currentPage = 1;
let currentSearchData = {};







function searchAndDisplayResults() {
   
    const company = document.getElementById('company').value;
    const director = document.getElementById('director').value;
    const resultFormat = document.getElementById('resultFormat').value;
    const numPages = parseInt(document.getElementById('numPages').value, 10);
    const csrfToken = document.getElementById('csrf_token').value;
    const companyColumn = document.querySelector('input[name="companyColumn"]:checked').value;
    const directorColumn = document.querySelector('input[name="directorColumn"]:checked').value;

    currentSearchData = {
        company: company,
        director: director,
        format: resultFormat,
        numPages: numPages,
        page: currentPage,
        companyColumn:companyColumn,
        directorColumn:directorColumn

    };

    // Show dynamic loader and keep the table visible
    
    document.querySelector('.static-loader').classList.add('loader'); 
    document.getElementById('loaderContainer').style.display = 'flex';

    fetch('/search', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': csrfToken  // Include CSRF token in the headers
        },
        body: JSON.stringify(currentSearchData)
        
    })
    .then(response => {
        
        console.log('Response received');  // Debug: Check if response is received
        if (!response.ok) {
            document.getElementById('loaderContainer').style.display = 'none';
            return response.json().then(errorData => {
                throw new Error(errorData.error || 'Unknown error occurred');
            });
        }
        return response.json();
    })
    .then(data => {

        document.getElementById('loaderContainer').style.display = 'none';
        document.getElementById('textContainer').style.display = 'none';
        console.log('Data:', data);  // Debug: Log the data received
        
        // Clear previous results
        let resultsTable;
        let resultsTableBody;
        if (resultFormat === 'Company') {
            document.getElementById('companyTable').style.display = 'table';
            document.getElementById('directorTable').style.display = 'none';
            document.getElementById('combinedTable').style.display = 'none';
            resultsTable = document.getElementById('companyTable')
        } else if (resultFormat === 'Director') {
            document.getElementById('companyTable').style.display = 'none';
            document.getElementById('directorTable').style.display = 'table';
            document.getElementById('combinedTable').style.display = 'none';
            resultsTable = document.getElementById('directorTable')
        } else {
            document.getElementById('companyTable').style.display = 'none';
            document.getElementById('directorTable').style.display = 'none';
            document.getElementById('combinedTable').style.display = 'table';
            resultsTable = document.getElementById('combinedTable')
        }

        resultsTableBody=resultsTable.getElementsByTagName('tbody')[0];
        resultsTableBody.innerHTML = '';

        // Append new results to the table
        data.results.forEach((row, index) => {
            const newRow = resultsTableBody.insertRow();
            const serialNumberCell = newRow.insertCell();
            serialNumberCell.textContent = (currentPage - 1) * numPages + index + 1; // Calculate serial number
            row.forEach(cellData => {
                const newCell = newRow.insertCell();
                newCell.textContent = cellData;
            });
        });

        document.getElementById('loaderContainer').style.display = 'none';
        
        // Update pagination controls
        document.getElementById('currentPage').textContent = currentPage;
        document.getElementById('prevPage').disabled = currentPage === 1;
        document.getElementById('nextPage').disabled = data.results.length < numPages;
       
    })
    .catch(error => {
        document.getElementById('loaderContainer').style.display = 'none'; 
        alert('Error: ' + error.message);
    });
}


function fetchData(event) {
    event.preventDefault();  // Prevent the form from submitting the default way
    currentPage = 1;
    searchAndDisplayResults();
}

function changePage(delta) {
    currentPage += delta;
    searchAndDisplayResults();
}

window.onload = function() {
    document.getElementById('loaderContainer').style.display = 'none';
    document.getElementById('companyTable').style.display = 'none';
    document.getElementById('directorTable').style.display = 'none';
    document.getElementById('combinedTable').style.display = 'none';
}
