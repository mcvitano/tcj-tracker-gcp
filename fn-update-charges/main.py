def process_yesterdays_charges(request):
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
    destinationBlob = f'charges/{datetime.date.today()}.pdf'
    bucketName = os.environ['bucketName']
    bucket = storageClient.get_bucket(bucketName)
    blob = bucket.blob(destinationBlob)

    bigqueryClient = bigquery.Client()
    datasetName = os.environ['datasetName']

    try:
        # download charge report (PDF)
        sourceFile = 'https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF/01.PDF'
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
                  'source': ['charges'],
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

        return 'Failed to download charge report.'

    try:
        #########################
        # PARSE PDF TO DATAFRAME
        #
        #########################
        # create a PDF reader
        pdfReader = PyPDF2.PdfFileReader(io.BytesIO(pdf))

        def _extract_fields(s):
            """Extract data fields from text.
            Args:
                s (text extracted between two inmate's names): string
            Returns:
                dict
            """
            # name
            name_ = booked_names[i]
            m = re.search(f'({name_})', s)
            name = m.group().strip() if m else ''

            # cid
            cid_ = '(?<!-)(\d{7,})'
            m = re.search(cid_, s)
            cid = m.group().strip() if m else '0000000'

            # address
            address_ = f'{booked_names[i]}(.*)(?<={cid})'
            m = re.search(f'{address_}', s)
            address = m.group(1).replace(cid, '').strip() if m else ''

            # date
            charge_date = datetime.date.today() - datetime.timedelta(1)

            # booking id
            charge_ = '\d\d-\d\d\d\d\d\d+'

            charge_id_list = list(dict.fromkeys(re.findall(charge_, s)))
            charges_text = []
            for j in range(0, len(charge_id_list)):
                if j+1 < len(charge_id_list):
                    this_charge_text = re.findall(f'{charge_id_list[j]}(.*){charge_id_list[j+1]}', s)[0].strip()
                    charges_text.append(re.sub(charge_id_list[j], ',', this_charge_text).strip())
                else:
                    this_charge_text = re.findall(f'{charge_id_list[j]}(.*)', s)[0].strip()
                    charges_text.append(re.sub(charge_id_list[j], ',', this_charge_text).strip())

            # pool
            charges_text = [[k][0].split(',') for k in charges_text]
            # flatten
            charges_text = [x for xs in charges_text for x in xs]

            field_dict = {
                'name': name,
                'address': address,
                'cid': cid,
                'charge_date': charge_date,
                'charge_id': charge_id_list,
                'charge_count': len(charges_text),
                'charge_list': charges_text
                }

            return field_dict

        # empty database (to fill)
        df = pd.DataFrame()

        # iterate over pages in PDF
        for page in range(0, pdfReader.numPages):
            # create a page object
            pageObj = pdfReader.getPage(page)

            # extract text from page
            pageText = pageObj.extractText()

            # get text as string (remove EOL characters)
            text_spaced = pageText.replace('\n\n', ' ').strip()

            # get list of names (persons charged; minimum 2 letter last name)
            booked_names = re.findall('\s([A-Z][A-Z]+,\s[A-Z\s]*)[0-9]', text_spaced)

            # iterate over names
            # extract all text between name[i] and name[i+1]
            rows_list = []
            for i in range(0, len(booked_names)):
                if i+1 < len(booked_names):
                    s = re.search(f'({booked_names[i]}.*){booked_names[i+1]}', text_spaced)

                else:
                    s = re.search(f'({booked_names[i]}.*)Inmates Booked', text_spaced)

                fields = _extract_fields(s.group(1))
                rows_list.append(fields)

            # append all extracted rows to empty database
            df = pd.concat([df, pd.DataFrame(rows_list)])
            df.reset_index(inplace=True, drop=True)

        # Person-level grouping
        #   Sometimes the address and CID get cutoff when moving to a new page in the PDF report.
        #   Grouping by [name] is dangerous since some names are common (and stored in alphabetical order).
        #   The solution below is to simply fill missing values forward by name, starting with the CID
        #       as the address may also be missing for different reasons.
        # Example:
        #	WILLIAMS, TARIK LEROYBYRD	3370 ALDER AVE FREMONT CA 94536	1025105	2024-01-02	[24-0123883]	1	POSS CS PG 1/1-B >=4G<200G , POSS CS PG 2 >= 4...
        #   WILLIAMS, TARIK LEROYBYRD		                            0000000	2024-01-02	[24-0123883]	1	POSS MARIJ >4OZ<=5LBS
            
        # set 'empty' and cid== 0000000 to missing/NULL
        df.replace('0000000', np.NaN, inplace=True)
        df.replace('', np.NaN, inplace=True)

        # fill forward
        df['cid'] = df.groupby('name')['cid'].ffill()
        df['address'] = df.groupby(['name', 'cid'])['address'].ffill()

        # convert to string and combine over lines
        df['charge_id_string'] = df['charge_id'].map(lambda x: ', '.join(x))
        df['charge_id_string'] = df.groupby(['cid'])['charge_id_string'].transform(lambda x: ','.join(list(dict.fromkeys(x))))

        # convert to string and combine over lines
        df['charge_list_string'] = df['charge_list'].map(lambda x: ', '.join(x))
        df['charge_list_string'] = df.groupby(['cid'])['charge_list_string'].transform(lambda x: ','.join(list(dict.fromkeys(x))))

        # recalculate
        df['charge_count'] = [len(x.split(',')) for x in df['charge_list_string']]

        # drop columns so that duplicates may be identified
        df.drop(columns=['charge_id', 'charge_list'], inplace=True)

        # drop duplicates (one row per person)
        df.drop_duplicates(keep='first', inplace=True)
        df.reset_index(inplace=True, drop=True)

        # recreate/convert string columns to list[] fields (and array in BigQuery)
        df['charge_id'] = df['charge_id_string'].str.split(',')
        df['charge_list'] = df['charge_list_string'].str.split(',')

        # drop unneeded columns
        df.drop(columns=['charge_id_string', 'charge_list_string'], inplace=True)

        ############################
        # IMPORT DATA INTO BIGQUERY
        #
        ############################
        tableId = f'{datasetName}.charges'

        jobConfig = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,

            schema=[
                bigquery.SchemaField("name", "STRING", "REQUIRED"),
                bigquery.SchemaField("address", "STRING"),
                bigquery.SchemaField("cid", "STRING"),
                bigquery.SchemaField("charge_date", "DATE", "REQUIRED"),
                bigquery.SchemaField("charge_id", "STRING", "REPEATED"),
                bigquery.SchemaField("charge_count", "INTEGER"),
                bigquery.SchemaField("charge_list", "STRING", "REPEATED"),
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
                  'source': ['charges'],
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

        return 'Failed to parse charge report.'
