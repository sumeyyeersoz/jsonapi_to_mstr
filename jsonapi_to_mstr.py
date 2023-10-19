#from json import loads
from mstrio.connection import Connection
from mstrio.project_objects import SuperCube
from datetime import datetime
import pickle
import requests
import json
import base64
import pandas as pd

#for certification error
import urllib3
urllib3.disable_warnings()

pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 1000)


def authenticate():
    try:
        # API endpoint URL
        auth_url = "XXXX"
 
        # Authorization
        payload = {
            "username": "xxxx",#CHANGE
            "password": "xxxx"#CHANGE
       }
 
        # Headers settings
        headers = {
            "accept": "*/*",
            "Accept-Language": "tr",
            "Content-Type": "application/json"
        }
 
        # POST request
        response_auth = requests.post(auth_url, json=payload, headers=headers)
 
        # check api answer
        if response_auth.status_code == 200:
            # get json data
            auth_data = response_auth.json()
 
            # check the data
            if "data" in auth_data and "token" in auth_data["data"]:
                access_token = auth_data["data"]["token"]
                return access_token
            else:
                return "Error: Can't find the token!"
        else:
            return f"Authorization failed! HTTP code: {response_auth.status_code}"
 
    except Exception as e:
        return f"Error: {e}"
 
# call function:
try:
    access_token = authenticate()  # get the access_token
    #print("Access Token:", access_token)  
 
    # API endpoint URL
    url = 'XXXX'#CHANGE
 
    try:
        # GET REQUEST AND ADD TOKEN 
        response = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
 
        # if success
        if response.status_code == 200:
            # get JSON
            data= response.json()
            df = pd.DataFrame(data)

            # Print the DataFrame
            #print(df)
            print("Get data from API successed.")
            #return data
        else:
            print("Error: HTTP code:", response.status_code)
 
    except Exception as e:
        print("Error:", e)
 
except Exception as e:
    print("Error:", e)

#Connect Microstrategy and push data to a Cube.
### MSTRIO
# Authentication & Connection
mstr_base_url = "xxxx/MicroStrategyLibrary" #CHANGE with your mstr link
mstr_project_id = "xxxx" #CHANGE
mstr_username = " administrator" #CHANGE
mstr_password =  "xxxx" #CHANGE
mstr_login_mode = 1       
mstr_folder="XXXX" # CHANGE the cube creating folder
cube_name = "Guvenlik_Gecis_Cube" #CHANGE #If you want to create a cube. give the name of the new cube.
cube_repository = "Guvenlik_Gecis_Cube.pkl" #CHANGE
dataset_id="xxxx"#CHANGE #If you want to update spesific cube give the id.


### FUNCTIONS
def authenticate_mstr():
    conn = Connection(mstr_base_url, mstr_username, mstr_password, project_id=mstr_project_id, login_mode=mstr_login_mode
                            , ssl_verify=False)
    conn.connect()
    return conn
	
def create_cube(conn, data, cube_name,cube_repository):
    try:
        dataset = SuperCube(connection=conn, name=cube_name)
        dataset.add_table(name=cube_name, data_frame=df, update_policy="replace")#,
                          #to_attribute=to_attrib_tup, to_metric=to_metric_tup)
        dataset.create(folder_id=mstr_folder)
        dataset_id = dataset.id # retrieve ID of the new cube
    except:
        print("Problem with connection to MSTR or with cube creation")
        
    try:
        f = open(cube_repository,"wb") # save cube ID to a file
        pickle.dump(dataset_id,f)
        f.close()
    except:
        print("Problem with creating a pickle file")	
		
		
def update_cubes(conn,df,dataset_id):
    try:
        dataset = SuperCube(connection=conn, id=dataset_id)
        dataset.add_table(name=cube_name, data_frame=df, update_policy="update") #update
        dataset.update()
        print("Data push successful")
    except:
        print("Problem with update data")
    

conn= authenticate_mstr()

# mstr cube creation
#create_cube(conn,df,cube_name,cube_repository)

# mstr cube update
update_cubes(conn,df,dataset_id)