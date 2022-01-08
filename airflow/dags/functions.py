'''from datetime import date

def process_data(**kwargs):
    file = open("/home/repl/workspace/processed_data-" + kwargs['ds'] + ".tmp", "w")
    file.write(f"Data processed on {date.today()}")
    file.close()'''

import pandas as pd
import json
import urllib3
import os


def my_func():
    print('Hello from my_func')
    return 'hello from my_func'


def print_context(**context):
    print(context)


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
    """Retrieve aemet hourly data for given outputs."""
    
    api_key = kwargs['aemet_api_key']
    target_folder = kwargs['target_datalake'] + "/raw/aemet/predictions/hourly/" + kwargs["ts_nodash"]
    aemet_codigos = kwargs['aemet_codigos']

    # communicate this data to next task
    kwargs['task_instance'].xcom_push(key='target_folder', value=target_folder)

    # Create the folder for the batch.
    os.makedirs(target_folder, exist_ok=True)

    # Get the count of correct downloads.
    c_exp = len(aemet_codigos)
    c = 0

    # print("Data args input: api [ {} ], folder [ {} ], codigos [ {} ], ts_nodash [ {} ]".format(api_key, target_folder, aemet_codigos, kwargs["ts_nodash"]))

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
    input_folder = kwargs['task_instance'].xcom_pull(key='target_folder', task_ids='get_aemet_hourly_predictions')

    # Folder to write output dataframe.
    target_folder = kwargs['target_datalake'] + "/transformed/aemet/predictions/hourly/" + kwargs["ts_nodash"]
    
    j = None
    with open(f"{input_folder}/18153.json", "r", encoding='latin-1') as fo:
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
    res_main.to_csv(f"{target_folder}/18153.json", index=False, header=True)

    return "Task finished"

