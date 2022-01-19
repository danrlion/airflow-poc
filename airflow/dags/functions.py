#from shelve import DbfilenameShelf
from datetime import datetime
import pandas as pd
import psycopg2
import urllib3
import json
import os


'''def my_func():
    print('Hello from my_func')
    return 'hello from my_func'


def print_context(**context):
    print(context)'''


def getReqAemet(**kwargs):
    """Send a request following Aemet procedure and return the data requested."""

    # First check the proper arguments have been given.
    if [x for x in ['target_url', 'api_key'] if x not in kwargs.keys()]:
        return None
    
    api_key = kwargs['api_key']
    target_url = kwargs['target_url']
    
    # Set up http request maker thing
    http = urllib3.PoolManager()
    # Send request.
    response = http.request("GET", target_url, fields={"api_key": api_key})

    if response.status != 200:

        # logger.error("getReqAemet::First response not '200' with {}, response='{}'".format(kwargs["filename"], response.status))
        return None

    else:
        # Get the content of the response.
        content = json.loads(response.data.decode('ISO-8859-1'))
        
        if "datos" not in content.keys():
            # logger.error("getReqAemet::'datos' in response.data not found with {}: '{}'".format(kwargs["filename"], str(content.items())))
            return None

        # (else) Create API call for data.
        url_data = content["datos"]
        response = http.request("GET", url_data, fields={"api_key": api_key})

        if response.status != 200:

            # logger.error("getReqAemet::Second response not '200' with {}, response='{}'".format(kwargs["filename"], response.status))
            return None

        elif response.data is None:
            # logger.error('No data found or could not be requested!')
            return None

        else:    
            
            # logger.info("getReqAemet::Downloaded {}".format(kwargs["filename"]))
            return response.data


def getPredictionsAemetHourly(**kwargs):
    """Retrieve aemet hourly data for given stations."""
    
    # Get time now and set up timestamp.
    dt = datetime.now()
    stamp = "{:04d}{:02d}{:02d}T{:02d}{:02d}{:02d}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)

    # Set up variables.
    api_key = kwargs['aemet_api_key']
    target_folder = kwargs['target_datalake'] + "/raw/aemet/predictions/hourly/" + stamp
    aemet_codigos = kwargs['aemet_codigos']

    # communicate this data to next task
    kwargs['task_instance'].xcom_push(key='target_folder', value=target_folder)

    # Create the folder for the batch.
    os.makedirs(target_folder, exist_ok=True)

    # Get the count of correct downloads.
    c_exp = len(aemet_codigos)
    c = 0

    # print("Data args input: api [ {} ], folder [ {} ], codigos [ {} ], stamp [ {} ]".format(api_key, target_folder, aemet_codigos, stamp))

    # Get API file for the parameters provided.
    for codigo in aemet_codigos:

        try:

            url = f"https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/{codigo}"

            # Send a API request to the url and info specified.
            data = getReqAemet(**{"target_url": url, "api_key": api_key})

            if data is None:
                
                print(f'No data found or could not be requested for {codigo} (#{c})!')
                continue

            else:

                # Put object in datalake bucket.
                with open(f'{target_folder}/{codigo}.json', 'wb') as f:
                    f.write(data)
                
                c += 1
                print(f"Continuing with codigo {codigo}")
    
        except Exception as e:
            
            # print("Excepcion: " + str(e))
            continue

    return "Task finished"


def transformPredictionsAemetHourly(**kwargs):
    """Extract info from json file for hourly aemet predictions."""
    
    # Get input data folder.
    input_folder = kwargs['task_instance'].xcom_pull(key='target_folder', task_ids='extract_aemet_hourly_predictions')

    # Folder to write output dataframe.
    target_folder = kwargs['target_datalake'] + "/transformed/aemet/predictions/hourly/" + input_folder.split("/")[-1]
    
    # communicate this data to next task
    kwargs['task_instance'].xcom_push(key='target_folder', value=target_folder)

    # Create the folder for the batch.
    os.makedirs(target_folder, exist_ok=True)

    # Do it for each file present in folder.
    for filename in os.listdir(input_folder):

        j = None
        with open(f"{input_folder}/{filename}", "r", encoding='latin-1') as fo:
            j = json.load(fo)

        if j is None:
            return None
        
        # Create dataframe as main result variable
        res_main = pd.DataFrame([])
    
        # Extract json part for predictions
        d = j[0]["prediccion"]["dia"]  
        
        for n in range(len(d)):
            
            # Create dataframe as sub (loop) result variable
            res_sub = pd.DataFrame([], columns=["parametro", "value", "periodo"])
            
            # First group with similar structure.
            for k in ("precipitacion", 'probPrecipitacion', 'probTormenta', 'nieve', 'probNieve', 'temperatura', 'sensTermica', 'humedadRelativa'):   
                
                # Flatten structure and add to dataset.
                u = pd.json_normalize(d[n], k)
                u["parametro"] = k
                res_sub = pd.concat([res_sub, u])
                
            # Second "group" with similar structure.
            # Flatten structure and add to dataset.
            u = pd.json_normalize(d[n], "estadoCielo")
            u["parametro"] = "estadoCielo"
            res_sub = pd.concat([res_sub, u])

            # Third "group" with similar structure.
            u = pd.json_normalize(d[n]["vientoAndRachaMax"][::2])
            # These two columns are a length-1 array, so extract values.
            u["direccion"] = [x[0] for x in u["direccion"]]
            u["velocidad"] = [x[0] for x in u["velocidad"]]
            # the structure here has even and odd difference which is resolved by joining the two parts.
            u = u.merge(pd.json_normalize(d[n]["vientoAndRachaMax"][1::2]), on="periodo")
            u["parametro"] = "vientoAndRachaMax"
            res_sub = pd.concat([res_sub, u])

            # Add the common attributes of this part.
            res_sub["fecha"] = d[n]["fecha"]
            res_sub["orto"] = d[n]["orto"]
            res_sub["ocaso"] = d[n]["ocaso"]
            
            # Finally concat this data to the main df.
            res_main = pd.concat([res_main, res_sub])
        
        # Main loop: add the common attributes of this part.
        res_main["elaborado"] = j[0]["elaborado"]
        res_main["nombre"] = j[0]["nombre"]
        res_main["provincia"] = j[0]["provincia"]
        
        # Write output df to folder.
        filename = filename.replace("json", "csv") 
        res_main.to_csv(
            f"{target_folder}/{filename}",
            index=False,
            header=True,
            quotechar="'",
            sep=";"
            )

    return "Task finished"


def insertPredictionsAemetHourly(**kwargs):
    """Insert data from files info database."""
    
    # Set up database connection.
    dbname = kwargs['dbname']
    dbuser = kwargs['dbuser']
    dbpass = kwargs['dbpass']

    # connect to db.
    dbconection = psycopg2.connect(f"dbname={dbname} user={dbuser} password={dbpass}")
    
    # Create cursor from conexion.
    dbcursor = dbconection.cursor()

    # Get input data folder.
    input_folder = kwargs['task_instance'].xcom_pull(key='target_folder', task_ids='transform_aemet_hourly_predictions')

    # Do it for each file present in folder.
    for filename in os.listdir(input_folder):

        with open(f"{input_folder}/{filename}", "r", encoding='UTF-8') as fo:
            
            # First line is header.
            csvheader = fo.readline().replace(";", ",").strip()

            # Better read line by line.
            for line in fo:
                
                # Format according to sql: replace missing with NULL(postgresql), ";" by ",".
                line = ["NULL" if len(x) == 0 else x for x in line.strip().split(";")]

                try:

                    dbcursor.execute(
                        f"INSERT INTO prediccion_horaria_nonorm({csvheader}) "
                        + f"VALUES('" + "','".join(line) + "')"
                    )
                
                except Exception as excp:
                    
                    # Rollback last changes.
                    dbconection.rollback()
                    print(f">Exception: {excp} \n>Line of file {filename}: {line}")
    
        # commit changes produced by the file.
        dbconection.commit()
    
    # close the connection to db.
    dbconection.close()

    return "Task finished"
        