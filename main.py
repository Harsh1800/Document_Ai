import json
import os
import logging
from google.cloud import storage
from google.cloud import documentai_v1 as documentai
import functions_framework
import csv
import io
import fitz
from google.oauth2 import service_account

# Configure logging
logging.basicConfig(level=logging.INFO)

# Set environment variables
PROJECT_ID = "your project id"
OUTPUT_BUCKET_NAME = "Your Output Bucket"
PROCESSOR_ID = "Your Processor ID"
PROCESSOR_LOCATION = "us"  # Default to 'us' if not specified


SERVICE_ACCOUNT = "service_account_key.json"

# Load credentials from service account file
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT)

# Initialize clients with credentials
storage_client = storage.Client(credentials=credentials)
docai_client = documentai.DocumentProcessorServiceClient(credentials=credentials)

@functions_framework.http
def process_pdf(request):
    """Handles PDF processing and financial calculations"""
    try:
        request_json = request.get_json(silent=True)
        if not request_json or 'pdf_gcs_uri' not in request_json:
            return json.dumps({"error": "Missing PDF GCS URI in request"}), 400

        pdf_gcs_uri = request_json['pdf_gcs_uri']
        logging.info(f"Processing PDF from URI: {pdf_gcs_uri}")

        # Download and split the PDF
        pdf_content = gcs_download_file_from_uri(pdf_gcs_uri)
        pdf_chunks = split_pdf_into_chunks(pdf_content)

        extracted_data = {}

        for index, chunk in enumerate(pdf_chunks):
            logging.info(f"Processing chunk {index + 1} / {len(pdf_chunks)}")
            document = process_document(chunk)
            chunk_data = extract_fields(document)

            # Merge extracted fields
            for key, value in chunk_data.items():
                if key not in extracted_data:
                    extracted_data[key] = value
                else:
                    if isinstance(extracted_data[key], list) and isinstance(value, list):
                        extracted_data[key].extend(value)
                    elif isinstance(extracted_data[key], dict) and isinstance(value, dict):
                        extracted_data[key].update(value)
                    else:
                        extracted_data[key] = [extracted_data[key]] if not isinstance(extracted_data[key], list) else extracted_data[key]
                        extracted_data[key].append(value)

        # Convert lists to single values where applicable
        for key, values in extracted_data.items():
            if isinstance(values, list) and len(values) == 1:
                extracted_data[key] = values[0]

        # Perform financial calculations
        logging.info(f"Extracted data before calculations: {json.dumps(extracted_data, indent=2)}")
        calculated_data = perform_calculations(extracted_data)
        # final_data = {**extracted_data, **calculated_data}
        final_data = {
           "Extracted Data": extracted_data,
           "Calculated Metrics": calculated_data}

        logging.info(f"Final extracted and calculated data: {json.dumps(final_data, indent=2)}")

        # Upload results
        base_filename = os.path.basename(pdf_gcs_uri).rsplit('.', 1)[0]
        json_gcs_uri = upload_to_gcs(json.dumps(final_data, indent=2), f"{base_filename}.json", OUTPUT_BUCKET_NAME, 'json')
        csv_gcs_uri = upload_to_gcs(json.dumps(final_data, indent=2), f"{base_filename}.csv", OUTPUT_BUCKET_NAME, 'csv')

        return json.dumps({
            "json_gcs_uri": json_gcs_uri,
            "csv_gcs_uri": csv_gcs_uri,
            "extracted_data": final_data,
            "Calculated Metrics": calculated_data
        })

    except Exception as e:
        logging.error(f"Error processing PDF: {str(e)}", exc_info=True)
        return json.dumps({"error": str(e)}), 500

def gcs_download_file_from_uri(gcs_uri):
    """Downloads a file from Google Cloud Storage using a GCS URI."""
    try:
        bucket_name = gcs_uri.split('/')[2]
        blob_name = '/'.join(gcs_uri.split('/')[3:])
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_bytes()
    except Exception as e:
        logging.error(f"Error downloading file from URI {gcs_uri}: {str(e)}", exc_info=True)
        raise


def split_pdf_into_chunks(pdf_content):
    """Splits a PDF into 15-page chunks."""
    doc = fitz.open(stream=pdf_content, filetype="pdf")
    chunks = []
    
    for i in range(0, len(doc), 15):
        pdf_writer = fitz.open()
        for j in range(i, min(i + 15, len(doc))):
            pdf_writer.insert_pdf(doc, from_page=j, to_page=j)
        
        chunk_bytes = pdf_writer.tobytes()
        chunks.append(chunk_bytes)
    
    return chunks

def process_document(pdf_chunk):
    """Processes a single PDF chunk with Document AI."""
    try:
        name = f"projects/{PROJECT_ID}/locations/{PROCESSOR_LOCATION}/processors/{PROCESSOR_ID}"
        raw_document = documentai.RawDocument(content=pdf_chunk, mime_type='application/pdf')
        request = documentai.ProcessRequest(name=name, raw_document=raw_document)
        result = docai_client.process_document(request=request)
        return result.document
    except Exception as e:
        logging.error(f"Error processing document: {str(e)}", exc_info=True)
        raise

def extract_fields(document):
    """Extracts fields including parent-child relationships from Document AI response."""
    
    extracted_data = {}

    def process_entity(entity, parent=None):
        """Recursively processes entities and their child fields."""
        field_name = entity.type_
        field_value = entity.mention_text

        logging.debug(f"Processing entity: {field_name} = {field_value}")  # Log each field being processed

        if parent:
            # Ensure parent is always a dictionary before adding children
            if parent not in extracted_data or not isinstance(extracted_data[parent], dict):
                extracted_data[parent] = {}  # Convert existing string to dict

            extracted_data[parent][field_name] = field_value
        else:
            # Handle case where a field appears both as a parent and a standalone field
            if field_name in extracted_data:
                # If it already exists as a string, convert it to a dictionary
                if not isinstance(extracted_data[field_name], dict):
                    extracted_data[field_name] = {"value": extracted_data[field_name]}
                # extracted_data[field_name]["child"] = field_value
            else:
                extracted_data[field_name] = field_value

        # Process child entities recursively
        if hasattr(entity, "properties"):
            for child in entity.properties:
                process_entity(child, field_name)  # Pass current entity as parent

    # Loop through all top-level entities
    for entity in document.entities:
        process_entity(entity)

    logging.debug(f"Extracted data: {json.dumps(extracted_data, indent=2)}")  # Log the final extracted data

    return extracted_data

def perform_calculations(data):
    """Performs financial calculations"""
    calculations = {}

    def safe_convert(value):
        """Convert numbers (including from lists and nested dicts) to float safely, removing $ and commas."""
        if isinstance(value, dict):
            value = value.get("value", None)
        if isinstance(value, list):
            converted_values = []
            for v in value:
                if isinstance(v, str):
                    v = v.replace('$', '').replace(',', '')
                if isinstance(v, (int, float)) or (isinstance(v, str) and v.replace('.', '', 1).isdigit()):
                    try:
                        converted_values.append(float(v))
                    except:
                        pass
            if not converted_values:
                logging.warning(f"No valid numbers found in list: {value}")
            return converted_values[0] if converted_values else None
        if isinstance(value, str):
            value = value.replace('$', '').replace(',', '')
            if value.replace('.', '', 1).isdigit():
                return float(value)
        elif isinstance(value, (int, float)):
            return float(value)
        return None

    # Extract key financial fields safely from nested data
    total_assets = safe_convert(data.get('Assets', {}).get('Total-Assets'))
    cash_and_cash_equivalents = safe_convert(data.get('Assets', {}).get('Cash-and-cash-Equivalents'))
    fixed_assets = safe_convert(data.get('Assets', {}).get('Fixed-Assets'))
    investments = safe_convert(data.get('Assets', {}).get('Investments'))
    loans_and_advances = safe_convert(data.get('Assets', {}).get('Loans-and-Advances'))
    other_assets = safe_convert(data.get('Assets', {}).get('Other-Assets'))
    total_current_assets = safe_convert(data.get('Assets', {}).get('Total-Current-Assets'))
    total_non_current_assets = safe_convert(data.get('Assets', {}).get('Total-Non-current-Assets'))
    bills_and_collection = safe_convert(data.get('Assets', {}).get('Bills-and-Collection'))
    contingent_liabilities = safe_convert(data.get('Assets', {}).get('Contingent-liabilities'))

    total_liabilities = safe_convert(data.get('Liabilities', {}).get('Total-Liabilities'))
    accounts_payable = safe_convert(data.get('Liabilities', {}).get('Accounts-Payable'))
    borrowings = safe_convert(data.get('Liabilities', {}).get('Borrowings'))
    capital = safe_convert(data.get('Liabilities', {}).get('Capital'))
    deposits = safe_convert(data.get('Liabilities', {}).get('Deposits'))
    liabilities_and_provisions = safe_convert(data.get('Liabilities', {}).get('Liabilities-and-Provisions'))
    long_term_debt = safe_convert(data.get('Liabilities', {}).get('Long-term-debt'))
    other_current_liabilities = safe_convert(data.get('Liabilities', {}).get('Other-Current-liabilities'))
    reserves_and_surplus = safe_convert(data.get('Liabilities', {}).get('Reserves-and-Surplus'))
    taxes = safe_convert(data.get('Liabilities', {}).get('Taxes'))

    net_profit = safe_convert(data.get('Profit-Loss-Statement', {}).get('Net-Profit'))
    ebitda = safe_convert(data.get('Profit-Loss-Statement', {}).get('EBIDTA'))
    total_expenses = safe_convert(data.get('Profit-Loss-Statement', {}).get('Total-Expenses'))
    total_income = safe_convert(data.get('Profit-Loss-Statement', {}).get('Total-Income'))
    total_revenue = safe_convert(data.get('Profit-Loss-Statement', {}).get('Total-Revenue'))
    total_equity = safe_convert(data.get('Profit-Loss-Statement', {}).get('Total-Equity'))

    year = data.get('Year')
    if isinstance(year, dict):
        year = year.get("value")

    logging.info(f"Calculations for year: {year}")

    # Perform calculations
    if total_assets and total_liabilities:
        calculations['Debt-to-Assets-Ratio'] = round(total_liabilities / total_assets, 2)

    if total_liabilities and total_equity:
        calculations['Debt-to-Equity-Ratio'] = round(total_liabilities / total_equity, 2)

    if net_profit and total_assets:
        calculations['Return on Assets'] = round(net_profit / total_assets, 2)

    if net_profit and total_equity:
        calculations['Return on Equity'] = round(net_profit / total_equity, 2)

    if net_profit and total_revenue:
        calculations['Net Profit Margin'] = round((net_profit / total_revenue) * 100, 2)

    if total_revenue and total_assets:
        calculations['Total Asset Turnover Ratio'] = round(total_revenue / total_assets, 2)

    if ebitda and total_revenue:
        calculations['EBITDA Margin'] = round((ebitda / total_revenue) * 100, 2)

    if borrowings and ebitda:
        calculations['Debt Service Coverage Ratio'] = round(ebitda / borrowings, 2)

    if total_expenses and total_revenue:
        calculations['Expense-to-Revenue Ratio'] = round(total_expenses / total_revenue, 2)

    if total_revenue and total_expenses:
        calculations['Gross Profit Margin'] = round((total_revenue - total_expenses) / total_revenue * 100, 2)

    if total_liabilities and total_assets:
        calculations['Leverage Ratio'] = round(total_liabilities / total_assets, 2)

    if total_assets and total_equity:
        calculations['Equity Multiplier'] = round(total_assets / total_equity, 2)

    logging.info(f"Extracted values: Total-Assets={total_assets}, Total-Liabilities={total_liabilities}, Total-Equity={total_equity}, Net-Profit={net_profit}, Total-Revenue={total_revenue}, Total-Expenses={total_expenses}")
    logging.info(f"Calculated Financial Metrics: {json.dumps(calculations, indent=2)}")

    return calculations

def safe_get_value(obj):
    """Safely extract a 'value' from a dict or first dict in a list, or return the raw value if it's a str."""
    if isinstance(obj, dict):
        return obj.get("value", "")
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict) and "value" in item:
                return item["value"]
    elif isinstance(obj, str):
        return obj
    return ""

def json_to_csv(json_data):
    """Converts JSON data to a structured CSV format with parent and subcolumns."""
    output = io.StringIO()
    
    headers = [
        ("Year", ""),
        ("Assets", "Total Assets"),
        ("Assets", "Cash and Cash Equivalents"),
        ("Assets", "Fixed Assets"),
        ("Assets", "Investments"),
        ("Assets", "Loans-and-Advances"),
        ("Assets", "Other Assets"),
        ("Assets", "Total Current Assets"),
        ("Assets", "Total Non-current Assets"),
        ("Liabilities", "Total Liabilities"),
        ("Liabilities", "Accounts Payable"),
        ("Liabilities", "Borrowings"),
        ("Liabilities", "Capital"),
        ("Liabilities", "Deposits"),
        ("Liabilities", "Liabilities-and-Provisions"),
        ("Liabilities", "Other-Current-liabilities"),
        ("Liabilities", "Long-term Debt"),
        ("Liabilities", "Reserves and Surplus"),
        ("Profit & Loss", "Net Profit"),
        ("Profit & Loss", "EBITDA"),
        ("Profit & Loss", "Total Revenue"),
        ("Profit & Loss", "Total Expenses"),
        ("Profit & Loss", "Total-Income"),
        ("Calculated Metrics", "Debt-to-Assets Ratio"),
        ("Calculated Metrics", "Debt-to-Equity Ratio"),
        ("Calculated Metrics", "Return on Assets"),
        ("Calculated Metrics", "Return on Equity"),
        ("Calculated Metrics", "Net Profit Margin"),
        ("Calculated Metrics", "Total Asset Turnover Ratio"),
        ("Calculated Metrics", "EBITDA Margin"),
        ("Calculated Metrics", "Debt Service Coverage Ratio"),
        ("Calculated Metrics", "Expense-to-Revenue Ratio"),
        ("Calculated Metrics", "Gross Profit Margin"),
        ("Calculated Metrics", "Leverage Ratio"),
    ]
    
    writer = csv.writer(output)
    writer.writerow([header[0] for header in headers])  # Parent columns
    writer.writerow([header[1] for header in headers])  # Subcolumns
    
    extracted_data = json_data.get("Extracted Data", {})
    calculated_metrics = json_data.get("Calculated Metrics", {})

    def g(*path):
        data = extracted_data
        for key in path:
            if isinstance(data, dict):
                data = data.get(key, {})
            else:
                return ""
        return safe_get_value(data)

    def gm(key):
        return calculated_metrics.get(key, "")  # Directly return since values are already flat

    row = [
        safe_get_value(extracted_data.get("Year")),
        g("Assets", "Total-Assets"),
        g("Assets", "Cash-and-cash-Equivalents"),
        g("Assets", "Fixed-Assets"),
        g("Assets", "Investments"),
        g("Assets", "Loans-and-Advances"),
        g("Assets", "Other-Assets"),
        g("Assets", "Total-Current-Assets"),
        g("Assets", "Total-Non-current-Assets"),
        g("Liabilities", "Total-Liabilities"),
        g("Liabilities", "Accounts-Payable"),
        g("Liabilities", "Borrowings"),
        g("Liabilities", "Capital"),
        g("Liabilities", "Deposits"),
        g("Liabilities", "Liabilities-and-Provisions"),
        g("Liabilities", "Other-Current-liabilities"),
        g("Liabilities", "Long-term Debt"),
        g("Liabilities", "Reserves-and-Surplus"),
        g("Profit-Loss-Statement", "Net-Profit"),
        g("Profit-Loss-Statement", "EBIDTA"),
        g("Profit-Loss-Statement", "Total-Revenue"),
        g("Profit-Loss-Statement", "Total-Expenses"),
        g("Profit-Loss-Statement", "Total-Income"),
        gm("Debt-to-Assets-Ratio"),
        gm("Debt-to-Equity-Ratio"),
        gm("Return on Assets"),
        gm("Return on Equity"),
        gm("Net Profit Margin"),
        gm("Total Asset Turnover Ratio"),
        gm("EBITDA Margin"),
        gm("Debt Service Coverage Ratio"),
        gm("Expense-to-Revenue Ratio"),
        gm("Gross Profit Margin"),
        gm("Leverage Ratio"),
    ]
    
    writer.writerow(row)
    return output.getvalue()
    

def upload_to_gcs(data, filename, bucket_name, file_type):
    """Uploads a file to Google Cloud Storage."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(filename)

        if file_type == 'csv':
            # Ensure data is parsed to dict
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    logging.error(f"Invalid JSON string: {data}")
                    raise

            if not isinstance(data, dict):
                logging.error(f"Expected dict for CSV conversion, got {type(data).__name__}")
                raise TypeError("Data must be a dictionary for CSV conversion")

            csv_data = json_to_csv(data)
            blob.upload_from_string(csv_data, content_type='text/csv')
        else:
            blob.upload_from_string(data, content_type='application/json')

        return f"gs://{bucket_name}/{filename}"

    except Exception as e:
        logging.error(f"Error uploading file {filename} to bucket {bucket_name}: {str(e)}", exc_info=True)
        raise


