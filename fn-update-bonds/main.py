def process_yesterdays_bonds(request):
  from google.cloud import storage, bigquery
  import datetime

  import urllib.request
  import pandas as pd
  import numpy as np
  import PyPDF2
  import sys
  import re
  import io
  import os

  # Setup GCP resources
  storageClient = storage.Client()
  destinationBlob = f'bonds/{datetime.date.today()}.pdf'
  bucketName = os.environ['bucketName']
  bucket = storageClient.get_bucket(bucketName)
  blob = bucket.blob(destinationBlob)

  bigqueryClient = bigquery.Client()
  datasetName = os.environ['datasetName']

  try:
    # download charge report (PDF)
    sourceFile = 'https://cjreports.tarrantcounty.com/Reports/BondsIssued/FinalPDF/01.PDF'
    pdf = urllib.request.urlopen(sourceFile).read()

    # upload to Cloud Storage
    if not blob.exists():
      blob.upload_from_string(pdf, content_type='mime/pdf')

  except Exception as report_download_error:
    #exc_type, exc_obj, exc_tb = sys.exc_info()
    #fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #print(exc_type, fname, exc_tb.tb_lineno)
    errored_line_number = sys.exc_info()[2].tb_lineno

    df = pd.DataFrame(
      data={'run_date': [datetime.date.today()],
            'source': ['bonds'],
            'stage': ['download'],
            'error': [f'{report_download_error} (line {errored_line_number})']
            })

    tableId = f'{datasetName}.failed_jobs'
    jobConfig = bigquery.LoadJobConfig(
      write_disposition=bigquery.WriteDisposition.WRITE_APPEND,

      schema=[
        bigquery.SchemaField("run_date", "DATE", "REQUIRED"),
        bigquery.SchemaField("source", "STRING", "REQUIRED"),
        bigquery.SchemaField("stage", "STRING", "REQUIRED"),
        bigquery.SchemaField("error", "STRING", "REQUIRED"),
      ])

    job = bigqueryClient.load_table_from_dataframe(
      df, tableId, job_config=jobConfig
    )

    # Wait for the load job to complete.
    job.result()

    return 'Failed to download bond report.'

  try:
    ################################
    # PARSE PDF REPORT TO DATAFRAME
    #
    ################################
    # create a PDF reader
    pdfReader = PyPDF2.PdfFileReader(io.BytesIO(pdf))

    # inititate list of dataframes
    df_list = []

    # iterate over pages in PDF
    for page in range(0, pdfReader.numPages):

      # create a page object
      pageObj = pdfReader.getPage(page)

      # extract text from page
      pageText = pageObj.extractText()

      # extract names from text (last name must have at least two letters)
      # ... may also have LAST M, FIRST ... so need to account for the middle
      booked_names = re.findall('\n\n([A-Z][A-Z]+\s?[A-Z]*,\s+[A-Z ]*)\n\n', pageText)
      booked_names = [i.replace('\n\n', ' ').replace('  ', ' ').strip() for i in booked_names]

      # replace carriage returns --> one long piece of spaced text
      text_spaced = pageText.replace('\n\n', ' ').replace('  ', ' ').strip()

      # identify the start of each new "row" (bond)
      # (bond id [bond_status] charge)
      rows = re.findall('(\d{7,}\s[A-Z]{4}\s[0-9].*?)', text_spaced)

      # iterate over "rows" (bonds)
      data = []
      for i in range(0, len(rows)):

        # extract all text between start of row and start of next row
        if i + 1 < len(rows):
          s = re.search(f'({rows[i]}.*?){rows[i+1]}', text_spaced).group(1)
        else:
          s = re.search(f'({rows[i]}.*?)List of Bonds Issued', text_spaced).group(1)

        # regex for date (convenience)
        date_ = '\d\d?/\d\d?/\d\d\d\d'

        # extract report fields
        bond_id = re.search('(\d{7,})', s).group(1).strip()
        bond_status = re.search(f'{bond_id}(.*?)[0-9]', s).group(1).strip()
        bond_amount = re.search(f'{bond_status}\s([\d,.]+)', s).group(1).strip()
        cid = re.search(f'{bond_amount}\s(\d+)', s).group(1).strip()
        name = booked_names[i]
        charge = re.search(f'{name}\s(.*?){date_}', s).group(1).strip()
        bond_date = re.search(f'({date_})', s).group(1).strip()
        # [bondsman] is Title-case if a company name (vs. person)
        bondsman_match = re.search(f'{date_}\s([A-Za-z,\s]*){date_}', s)
        # when the bond_type is "Personal" or "Cash" there is no bondsman (empty)
        if bondsman_match:
          bondsman = bondsman_match.group(1).strip()
          if bondsman == '':
            bondsman = np.NaN
            charge_date = re.search(f'{date_}\s+({date_})', s).group(1).strip()
            # cast [bondsman] to upper as company names are given as Title-case
          else:
            charge_date = re.search(f'{bondsman}\s*({date_})', s).group(1).strip()
            bondsman = bondsman.upper()

        else:
          bondsman = np.NaN
          charge_date = re.search(f'{date_}\s+({date_})', s).group(1).strip()
        bond_type = re.search(f'{charge_date}\s([A-Za-z]+)\s', s).group(1).strip()
        address = re.search(f'{bond_type}\s(.*)', s).group(1).strip()

        # append all fields from row[i] to growing list
        row_data = [bond_id, bond_status, bond_amount, cid, name, charge,
                    bond_date, bondsman, charge_date, bond_type, address]
        data.append(row_data)

      # convert all data from page to dataframe and append to growing list
      df = pd.DataFrame(data)
      df_list.append(df)

    # combine all pages into a single dataframe
    df = pd.concat(df_list)
    df.columns = ['bond_id', 'bond_status', 'bond_amount', 'cid', 'name', 'charge',
                  'bond_date', 'bondsman', 'charge_date', 'bond_type', 'address']

    df['bond_date'] = pd.to_datetime(df['bond_date'])
    # set misformed date strings to np.NaN
    df['charge_date'] = pd.to_datetime(df['charge_date'], errors = 'coerce')

    ############################
    # IMPORT DATA INTO BIGQUERY
    #
    ############################
    tableId = f'{datasetName}.bonds'

    jobConfig = bigquery.LoadJobConfig(
      write_disposition=bigquery.WriteDisposition.WRITE_APPEND,

      schema=[
          bigquery.SchemaField("bond_id", "STRING", "REQUIRED"),
          bigquery.SchemaField("bond_status", "STRING"),
          bigquery.SchemaField("bond_amount", "STRING"),
          bigquery.SchemaField("cid", "STRING"),
          bigquery.SchemaField("name", "STRING"),
          bigquery.SchemaField("charge", "STRING"),
          bigquery.SchemaField("bond_date", "DATE"),
          bigquery.SchemaField("bondsman", "STRING"),
          bigquery.SchemaField("charge_date", "DATE"),
          bigquery.SchemaField("bond_type", "STRING"),
          bigquery.SchemaField("address", "STRING"),
        ])

    job = bigqueryClient.load_table_from_dataframe(
      df, tableId, job_config=jobConfig
    )

    # Wait for the load job to complete.
    job.result()

    return f'Finished loading {destinationBlob}.'

  except Exception as parsing_error:
    errored_line_number = sys.exc_info()[2].tb_lineno

    df = pd.DataFrame(
      data={'run_date': [datetime.date.today()],
            'source': ['bonds'],
            'stage': ['parsing'],
            'error': [f'{parsing_error} (line {errored_line_number})']
            })

    tableId = f'{datasetName}.failed_jobs'
    jobConfig = bigquery.LoadJobConfig(
      write_disposition=bigquery.WriteDisposition.WRITE_APPEND,

      schema=[
        bigquery.SchemaField("run_date", "DATE", "REQUIRED"),
        bigquery.SchemaField("source", "STRING", "REQUIRED"),
        bigquery.SchemaField("stage", "STRING", "REQUIRED"),
        bigquery.SchemaField("error", "STRING", "REQUIRED")
      ])

    job = bigqueryClient.load_table_from_dataframe(
      df, tableId, job_config=jobConfig
    )

    # Wait for the load job to complete.
    job.result()

    return 'Failed to parse bond report.'
