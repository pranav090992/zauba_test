from flask import Flask, request, jsonify, render_template
import time
from flask import *

import time
import pyodbc
from flask import Flask, render_template
cnxn = pyodbc.connect(r'Driver=SQL Server;Server=.\SQLEXPRESS;Database=zauba;Trusted_Connection=yes;')


app = Flask(__name__)
app.config['SECRET_KEY'] = 'your_secret_key'  # Replace with your actual secret key
# csrf = CSRFProtect(app)



def log(message):
    timestamp = time.strftime("%H:%M:%S %p")
    s = f"{timestamp} - {message}\n"
  
    try:
        with open("log.txt", "a") as log_file:
            log_file.write(s)
    except FileNotFoundError:
        with open("log.txt", "w") as log_file:
            log_file.write(s)
            


def generate_query_from_name(col, input_string,is_id):
    query = ' '
    params = []
    if is_id:
        if col!='f.CIN':
            params.append(input_string.strip())
            query += f"{col} LIKE ? "
        else:
            params.append(input_string.strip())
            params.append(input_string.strip())
            params.append(input_string.strip())
            query += f" ( ISNULL(f.CIN, '') LIKE ? or ISNULL(f.LLP_Identification_Number, '') LIKE ? or ISNULL(f.Foreign_Company_Registration_Number, '') LIKE ? ) "
    else:
        params.append(f"{input_string.strip()}%")
        params.append(f"% {input_string.strip()}%")
        query += f" ({col} LIKE ? or {col} LIKE ? ) "
    print(query)
    return query, params      
        


class QueryError(Exception):
    """Custom exception for query errors."""
    pass





@app.route('/')
def home():
    # csrf_token = generate_csrf()
    return render_template('index.html')
    # return render_template('index.html', csrf_token=csrf_token)

@app.route('/search', methods=['POST'])
def search():
    companyQuery = request.json.get('company')
    directorQuery = request.json.get('director')
    result = request.json.get('format')
    num_pages = int(request.json.get('numPages'))
    page = int(request.json.get('page'))
    companyColumn = request.json.get('companyColumn')
    directorColumn = request.json.get('directorColumn')

    offset = (page - 1) * num_pages
    data = request.json

    # + goes for 'and' take intersection
    # | goes for 'or' take union
    # default it goes for SOP(sum of product) as here statements are like AB + C => can be goes as (A&B) U C. here + means as union.
    if not companyQuery and not directorQuery:
        return jsonify({'error': 'Both company and director cannot be empty.'}), 400
    
    base_query = "SELECT  f.CIN,  f.Company_Name, d.DIN, d.SN, d.Director_Name, d.Designation, d.Appointment_Date, f.Company_Status, f.ROC, f.LLP_Identification_Number, f.Registration_Number, f.Foreign_Company_Registration_Number,f.Company_Category, f.Company_Sub_Category, f.Class_of_Company, f.Date_of_Incorporation, f.Age_of_Company, f.Activity,f.Number_of_Members, f.Authorised_Capital, f.Paid_up_capital, f.Listing_status, f.Date_of_Last_Annual_General_Meeting,f.Date_of_Latest_Balance_Sheet, f.Email_ID, f.Address, f.Url,f.Date_of_last_financial_year_end_date_for_which_Annual_Return_filed,f.Date_of_last_financial_year_end_date_for_which_Statement_of_Accounts_and_Solvency_filed, f.Description_of_main_division,f.Main_division_of_business_activity_to_be_carried_out_in_India, f.Number_Of_Partners, f.Number_of_Designated_Partners,f.Total_Obligation_of_Contribution, f.Country_of_Incorporation, f.Type_Of_Office, f.As_on "
    
    if  result == 'Company':
        base_query = "SELECT  f.CIN,  f.Company_Name, f.Company_Status, f.ROC, f.LLP_Identification_Number, f.Registration_Number, f.Foreign_Company_Registration_Number,f.Company_Category, f.Company_Sub_Category, f.Class_of_Company, f.Date_of_Incorporation, f.Age_of_Company, f.Activity, f.Number_of_Members, f.Authorised_Capital, f.Paid_up_capital, f.Listing_status, f.Date_of_Last_Annual_General_Meeting, f.Date_of_Latest_Balance_Sheet, f.Email_ID, f.Address, f.Url, f.Date_of_last_financial_year_end_date_for_which_Annual_Return_filed, f.Date_of_last_financial_year_end_date_for_which_Statement_of_Accounts_and_Solvency_filed, f.Description_of_main_division, f.Main_division_of_business_activity_to_be_carried_out_in_India, f.Number_Of_Partners, f.Number_of_Designated_Partners, f.Total_Obligation_of_Contribution, f.Country_of_Incorporation, f.Type_Of_Office, f.As_on, f.SN "
        
    if result == 'Director':
        base_query = "SELECT d.DIN, d.SN , d.Director_Name, d.Designation, d.Appointment_Date "
    
    # db_connection = DBConnection()
    
    try:
        if not directorQuery and result == 'Company' :
            base_query += " FROM [zauba].[zauba].[full_details] as f where "
            try:
                sql_query, params = generate_query_from_name(f"f.{companyColumn}" , companyQuery,companyColumn == 'CIN')
                base_query+=sql_query
                base_query += " order by f.Company_Name ASC "
            except ValueError as e:
                raise QueryError(f"Invalid query: {e}")

        elif  not companyQuery and result == 'Director' :
            base_query += " FROM [zauba].[zauba].[director] as d WHERE "
            try:
                sql_query, params = generate_query_from_name(directorColumn, directorQuery,directorColumn=='DIN')
                base_query+=sql_query
                base_query += " order by d.Director_Name ASC "
            except ValueError as e:
                raise QueryError(f"Invalid query: {e}")
        else:
            if result == 'Join':
                base_query += " FROM [zauba].[zauba].[full_details] as f LEFT JOIN [zauba].[zauba].[director] as d ON f.SN = d.SN where "
            else:
                base_query += " FROM [zauba].[zauba].[full_details] as f INNER JOIN [zauba].[zauba].[director] as d ON f.SN = d.SN where "
                
            try:
                params=[]
                if companyQuery:
                    sql_query_on_company, params_on_company = generate_query_from_name(f"f.{companyColumn}" , companyQuery,companyColumn == 'CIN')
                    base_query+=sql_query_on_company
                    params = params_on_company

                if companyQuery and directorQuery:
                    base_query+=" AND "
                    
                if directorQuery:
                    sql_query_on_director, params_on_director = generate_query_from_name(f"d.{directorColumn}" , directorQuery,directorColumn == 'DIN')
                    base_query+=sql_query_on_director
                    params = params + params_on_director
                if result == 'Director':
                    base_query += " order by d.Director_Name ASC "
                else:    
                    base_query += " order by f.Company_Name ASC "
                
            except ValueError as e:
                raise QueryError(f"Invalid query: {e}")
            
        base_query += f" OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;"
        params.append(offset)
        params.append(num_pages)
        params = tuple(params)
        
        # print(base_query)
        # print(params)
        
    
        try:
            cursor = cnxn.cursor()
            # cursor.execute(fquery)
            cursor.execute(base_query, params)
            rows = cursor.fetchall()
        except Exception as e:
            raise QueryError(f'Error executing query: {str(e)}')
   
    
        data = [tuple(row) for row in rows]
      
       
        
        has_more = len(data) == num_pages
  
        return jsonify({
            'results': data,
            'hasMore': has_more
        })

    except ValueError as ve:
        return jsonify({'error': str(ve)}), 400
    except QueryError as qe:
        return jsonify({'erroryoyo': str(qe)}), 400
    except Exception as e:
        return jsonify({'error': 'An unexpected error occurred: ' + str(e)}), 500
  


if __name__ == '__main__':
    app.run(debug=True)
