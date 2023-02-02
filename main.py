import json
import singer
# TODO: import pytz
# TODO: import logging
import requests
from retry import retry
from decouple import config, Csv


class Connection(object):

    def __init__(self, url, token):

        self.url = url.rstrip('/')
        self.baseUrl = self.url + "/api"
        self.headers = {'Authorization': 'Bearer ' + token, 'Accept': 'application/json'}

        self.HISTORY_SCHEMA = {
            'type': [
                'null', 
                'object'
            ], 
            'additionalProperties': False, 
            'properties': {
                'task_id': {'type': ['null', 'string']}, 
                'author': {'type': ['null', 'string']}, 
                'field': {'type': ['null', 'string']}, 
                'prev_state': {'type': ['null', 'string']}, 
                'state': {'type': ['null', 'string']},
                'timestamp': {'type': ['null', 'number']}
            }
        }
        # task_id - author - field -  prev_state -  state - datetime

    @retry(requests.exceptions.ConnectionError, tries=3, delay=2)
    def parse_projects(self):
        # get all project database id's
        self.r = requests.get(self.baseUrl+'/admin/projects/?fields=name,id', headers=self.headers)
        return {project['id']: project['name'] for project in self.r.json()}

    @retry(requests.exceptions.ConnectionError, tries=5, delay=2)
    def parse_fields_values_types(self):
        # get all fields with data types, names, and project instances
        request = 'customFields?fields=fieldType(type,valueType),instances(project(id)),name'
        self.r = requests.get(self.baseUrl+'/admin/customFieldSettings/'+request, headers=self.headers)
        # before - generate default fields items
        a = {field['name']: [field['fieldType']['valueType'],field['instances']] for field in self.r.json()}
        b = {} # mapping below >> {project_id:[{field_name:field_type}]}
        for name,values in a.items():
            pid = values[1][0]['project']['id'] if len(values[1]) > 0 else None
            if pid not in b:
                b[pid] = []
            b[pid].append([name,values[0]])

        return b

    def generate_schema(self, map):
        # build a schema for data stream
        schema = {
            'type': [
                'null', 
                'object'
            ],
            'additionalProperties': False,
            'properties':   {
                #  << putting jam here
            }
        }

        # custom fields from map   
        raw_jam = {field[0]:{'type':['null', field[1]]} for field in map} 

        # grind data types for custom fields
        jam = self.convert_data_types(raw_jam)

        # non-custom fields
        jam['project'] = {'type':['string']} 
        jam['id'] = {'type':['string']}
        jam['summary'] = {'type':['string']}
        
        # wrap fields in schema template
        schema['properties'] = jam

        return schema

    def convert_data_types(self,jam):
        # make field types little less wrong
        correcter = {
            'state': 'string',
            'enum': 'string',
            'user': 'string',
            'float': 'number',
            'period': 'number',
            'version': 'string',
            'ownedField': 'string',
            'EnumBundleElement': 'string'
        }
        
        # less correct :> more corrent
        for particular in jam.keys():
            _type = jam[particular]['type'][1]
            if _type in correcter: jam[particular]['type'][1] = correcter[_type]

        return jam


    @retry(requests.exceptions.ConnectionError, tries=5, delay=2)
    def parse_project_issues(self,project):
        # get all issues id's for project
        self.r = requests.get(self.baseUrl+'/admin/projects/'+project+'/issues?$top=-1', headers=self.headers)
        return [issue['id'] for issue in self.r.json()]

    @retry(requests.exceptions.ConnectionError, tries=5, delay=2)
    def transfer_issue(self,project,schema,id):             
        # main ETL loop for issue
    
        # get all necessary data for issue id
        self.r = requests.get(self.baseUrl+'/issues/'+id+(
            '?fields=id,project(name),summary,customFields(name,value(name,value))'
            ), headers=self.headers, allow_redirects=True) 
        jas = json.loads(self.r.text)

        # read
        res = {'project': jas['project']['name'], 'id': jas['id'], 'summary': jas['summary']}
        for item in jas['customFields']:
            res[item['name']] = item['value'] if type(item['value']) is not dict else item['value']['name']
            if type(item['value']) is not dict:
                res[item['name']] = item['value'] if item['value'] else None
            else:
                res[item['name']] = item['value']['name'] if item['value']['name'] else None

        # grind
        jam = {field:res[field] for field in schema['properties'].keys() if field in schema['properties'].keys()}

        # write
        singer.write_record('issue_'+project.replace('-', ''), jam)

    @retry(requests.exceptions.ConnectionError, tries=5, delay=2)
    def transfer_issue_activities(self,id):
        # main ETL loop for issue history

        res = {}
        # get any history data for fields changes of issue id
        fields = 'fields=field(name),author(login),timestamp,added(name),removed(name)'
        categories = 'categories=CustomFieldCategory'
        self.r = requests.get(self.baseUrl+'/issues/'+id+'/activities?'+categories+'&'+fields, 
            headers=self.headers)
        changes = self.r.json()
        if not changes: return

        # read
        for ch in changes:
            res['task_id'] = id
            res['author'] = ch['author']['login']
            res['field'] = ch['field']['name']
            res['state'] = ch['added'] if isinstance((ch['added']),(str,type(None))) else ch['added'][0]['name']
            res['prev_state'] = ch['removed'] if isinstance((ch['removed']),(str,type(None))) else ch['removed'][0]['name']
            res['timestamp'] = ch['timestamp']
            
        # grind
        jam = {field:res[field] for field in self.HISTORY_SCHEMA['properties'].keys()}
    
        # write
        singer.write_record('activity', jam)

      
def run():
    
    # get connection
    yt = Connection('https://singer-export.youtrack.cloud/youtrack/', config('TOKEN'))

    # parse projects
    projects = yt.parse_projects()

    # parse fields map
    map = yt.parse_fields_values_types()

    # create activity stream
    singer.write_schema('activity', yt.HISTORY_SCHEMA,'')

    # >>>>>
    for project in projects:

        if project in config('PROJECT_EXCLUDE', cast=Csv()): continue

        issues = yt.parse_project_issues(project)
        if len(issues) == 0: continue
        schema = yt.generate_schema(map[project])
        singer.write_schema('issue_'+project.replace('-', ''), schema, 'id')

        for issue in issues:
            yt.transfer_issue(project,schema,issue)
            if project in config('HISTORY_EXCLUDE', cast=Csv()): continue 
            yt.transfer_issue_activities(issue)
    
if __name__ == '__main__':
    run()

