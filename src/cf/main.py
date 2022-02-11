from google.cloud import storage
import subprocess
import google.auth
import google.auth.transport.requests
import requests
import six.moves.urllib.parse
import google.auth
import google.auth.compute_engine.credentials
import google.auth.iam
from google.auth.transport.requests import Request
import google.oauth2.credentials
import google.oauth2.service_account
import json
import os

#Environment variables.
PROJECT_NAME = os.environ.get('PROJECT_NAME')
LOCATION     = os.environ.get('LOCATION')
COMPOSER_ENV = os.environ.get('COMPOSER_ENV')
DAG_NAME     = os.environ.get('DAG_NAME')

def caller(event, context):
    try:
        event_path=check_storage(context)
        result=""
        if event_path!='_FOLDER_':
            file_name = event["name"]
            print("file_name",file_name)
            webserver_web_ui = get_airflow_web_uri(PROJECT_NAME, LOCATION, COMPOSER_ENV)
            def_dag_name = DAG_NAME
            print(def_dag_name)
            run_params = "Project Name:%s, Location:%s, Composer Env:%s, Dag Name:%s, WebServer Web UI:%s" % (PROJECT_NAME, LOCATION, COMPOSER_ENV,def_dag_name,webserver_web_ui)
            print(run_params)
            client_id = connect_composer(PROJECT_NAME,LOCATION,COMPOSER_ENV)
            trigger_dag(client_id,event_path,def_dag_name,webserver_web_ui)
            result='Call Processed'
        else:
            result="No Call Processed"
    except Exception as e:
        print("Problem in trigger dag ",e)
    return result

def check_storage(context):
    try:
        print("File Triggered")
        event=context.resource
        event_path=""
        event_dump_data=json.dumps(event)
        event_json_data=json.loads(event_dump_data)
        path_details=event_json_data['name'].split('/buckets/')[1]
        if path_details.endswith('/'):
            event_path="_FOLDER_" 
        else:
            path_array=path_details.split('/')
            for path in path_array:
                if path!='objects':
                    event_path +="/"+path
            print("event_path : "+event_path)
    except Exception as e:
        print("Problem in checking storage ",e)
    return event_path

def connect_composer(project_name,location,composer_env):
    try:
        credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        authed_session = google.auth.transport.requests.AuthorizedSession(credentials)
        environment_url = ('https://composer.googleapis.com/v1beta1/projects/{}/locations/{}/environments/{}').format(project_name, location, composer_env)
        composer_response = authed_session.request('GET', environment_url)
        environment_data = composer_response.json()
        airflow_uri = environment_data['config']['airflowUri']
        print(airflow_uri)
        redirect_response = requests.get(airflow_uri, allow_redirects=False)
        redirect_location = redirect_response.headers['location']

        # Extract the client_id query parameter from the redirect.
        parsed = six.moves.urllib.parse.urlparse(redirect_location)
        query_string = six.moves.urllib.parse.parse_qs(parsed.query)
        print("Client Id",query_string['client_id'][0])
        
    except Exception as e:
        print("Problem in composer connection ",e)
    return query_string['client_id'][0]

def trigger_dag(client_id,event_path,dag_name,webserver_web_ui):
    try:
        webserver_url = (webserver_web_ui+'/api/experimental/dags/'+ dag_name + '/dag_runs')
        make_iap_request(webserver_url, client_id, method='POST', json={'conf': '{"event_file_path": "'+str(event_path)+'"}'})
        
    except Exception as e:
        print("Problem in triggering dag ",e)
    return 'Success'    

def make_iap_request(url, client_id, method='GET', **kwargs):
    try:
        IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
        OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'
        if 'timeout' not in kwargs:
            kwargs['timeout'] = 90
        bootstrap_credentials, _ = google.auth.default(scopes=[IAM_SCOPE])
        bootstrap_credentials.refresh(Request())
        signer_email = bootstrap_credentials.service_account_email
        if isinstance(bootstrap_credentials,google.auth.compute_engine.credentials.Credentials):
            signer = google.auth.iam.Signer(Request(), bootstrap_credentials, signer_email)
        else:
            signer = bootstrap_credentials.signer
        
        print("Ouath Token URI",OAUTH_TOKEN_URI)
        service_account_credentials = google.oauth2.service_account.Credentials(
        signer, signer_email, token_uri=OAUTH_TOKEN_URI, additional_claims={'target_audience': client_id})
        google_open_id_connect_token = get_google_open_id_connect_token(service_account_credentials,OAUTH_TOKEN_URI)
        resp = requests.request(method, url,headers={'Authorization': 'Bearer {}'.format(google_open_id_connect_token)},**kwargs)
        if resp.status_code == 403:
            raise Exception('Service account {} does not have permission to access the IAP-protected application.'.format(signer_email))
        elif resp.status_code != 200:
            raise Exception('Bad response from application: {!r} / {!r} / {!r}'.format(resp.status_code, resp.headers, resp.text))
        else:
            return resp.text
        
    except Exception as e:
        print("Problem in make_iap_request ",e)
    return 'Success'    

def get_google_open_id_connect_token(service_account_credentials,OAUTH_TOKEN_URI):
    service_account_jwt = (service_account_credentials._make_authorization_grant_assertion())
    request = google.auth.transport.requests.Request()
    body = {
        'assertion': service_account_jwt,
        'grant_type': google.oauth2._client._JWT_GRANT_TYPE,
    }
    token_response = google.oauth2._client._token_endpoint_request(request, OAUTH_TOKEN_URI, body)
    return token_response['id_token']

def get_airflow_web_uri(project_id, location, composer_environment):
    credentials, _ = google.auth.default(
    scopes=['https://www.googleapis.com/auth/cloud-platform'])
    authed_session = google.auth.transport.requests.AuthorizedSession(
    credentials)

    environment_url = (
    'https://composer.googleapis.com/v1beta1/projects/{}/locations/{}'
    '/environments/{}').format(project_id, location, composer_environment)
    composer_response = authed_session.request('GET', environment_url)
    environment_data = composer_response.json()
    airflow_uri = environment_data['config']['airflowUri']
    return airflow_uri
