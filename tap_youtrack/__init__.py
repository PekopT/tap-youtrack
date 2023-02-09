#!/usr/bin/env python3
import json
import singer
import requests
from pytz import UTC
from retry import retry
from singer import utils
from datetime import datetime
from decouple import config, Csv

LOGGER = singer.get_logger()

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
            'additionalProperties': True, 
            'properties': {
                'task_id': {'type': ['null', 'string']}, 
                'author': {'type': ['null', 'string']}, 
                'field': {'type': ['null', 'string']}, 
                'prev_state': {'type': ['null', 'string']}, 
                'state': {'type': ['null', 'string']},
                'datetime': {'format': 'date-time','type': ['null', 'string']},
                'timestamp': {'type': ['number']}
            }
        }
        # task_id - author - field -  prev_state -  state - datetime


    @retry(requests.exceptions.ConnectionError, tries=config('TRIES'), delay=2)
    def parse_projects(self):
        # get all project database id's
        self.r = requests.get(self.baseUrl+'/admin/projects/?fields=name,id', headers=self.headers)
        return {project['id']: project['name'] for project in self.r.json()}


    @retry(requests.exceptions.ConnectionError, tries=config('TRIES'), delay=2)
    def parse_fields_values_types(self, simplified=False):
        # get all fields with data types, names, and project instances
        request = 'customFields?fields=fieldType(type,valueType),instances(project(id)),name'
        self.r = requests.get(self.baseUrl+'/admin/customFieldSettings/'+request, headers=self.headers)
        # before - generate default fields items
        a = {field['name']: [field['fieldType']['valueType'],field['instances']] for field in self.r.json()}
        b = {} # mapping below >> {project_id:[{field_name:field_type}]}
        
        if simplified:
            for name,values in a.items(): # if field have any instance
                if len(values[1]) > 0: b[name] = values[0]
            return b
            
        for name,values in a.items():
            pid = values[1][0]['project']['id'] if len(values[1]) > 0 else None
            if pid not in b:
                b[pid] = []
            b[pid].append([name,values[0]])
        return b

    
    def generate_schema(self, map, simplified=False):
        # build a schema for data stream
        schema = {
            'type': [
                'null', 
                'object'
            ],
            'additionalProperties': True,
            'properties':   {
                #  << putting jam here
            }
        }

        # custom fields from map
        if simplified:
            raw_jam = {key:{'type':['null',value]} for key,value in map.items()}   
        else:
            raw_jam = {field[0]:{'type':['null', field[1]]} for field in map} 

        # grind data types for custom fields
        jam = self.convert_data_types(raw_jam)

        # non-custom fields
        jam['timestamp'] = {'type':['number']} # using as _seq_id to target-postgres
        jam['project'] = {'type':['string']} 
        jam['id'] = {'type':['string']}
        jam['summary'] = {'type':['string']}
        jam['idReadable'] = {'type':['string']}
        jam['created']  = {'type': ['null', 'string'], 'format': 'date-time'}
        jam['resolved'] = {'type': ['null', 'string'], 'format': 'date-time'}
        jam['updated']  = {'type': ['null', 'string'], 'format': 'date-time'}
        jam['numberInProject'] = {'type':['number']}
        
        # wrap fields in schema template
        schema['properties'] = jam

        return schema  

    
    def make_catalog(self,schema):
    # Generate the catalog based on the retrieved schema information
        
        catalog = {
            "streams": [
                {
                    "stream": 'issue',
                    "tap_stream_id": 'issue',
                    "schema": schema,
                    "metadata": [
                        {
                            "breadcrumb": [],
                            "metadata": {
                                "table_name": 'issue',
                                "schema": "public",
                                "columns": [
                                    {
                                        "name": name,
                                        "type": types,
                                        "primary_key": True if name == "id" else False
                                    } for name, types in schema['properties'].items()
                                ],
                                "key_properties": [
                                    "id"
                                ],
                                "incremental": True,
                                "replication_method": "INCREMENTAL",
                                "version": "1.0.0",
                                "sequence": "timestamp"
                            }
                        }
                    ]
                },
                {
                    "stream": 'activity',
                    "tap_stream_id": 'activity',
                    "schema": self.HISTORY_SCHEMA,
                    "metadata": [
                        {
                            "breadcrumb": [],
                            "metadata": {
                                "table_name": 'activity',
                                "schema": "public",
                                "columns": [
                                    {
                                        "name": name,
                                        "type": types,
                                        "primary_key": True if name == "task_id" else False
                                    } for name, types in self.HISTORY_SCHEMA['properties'].items()
                                ],
                                "key_properties": [
                                    "task_id"
                                ],
                                "incremental": True,
                                "replication_method": "INCREMENTAL",
                                "version": "1.0.0",
                                "sequence": "timestamp"
                            }
                        }
                    ]
                }
            ]
        }
        return json.dumps(catalog,indent=4)

    
    def convert_ts(self,input):
        # from UNIX timestamp to reccommended by singer format
        if input is not None:
            dt = UTC.localize(datetime.utcfromtimestamp(input/1000))
            return dt.isoformat('T')

        
    def convert_data_types(self,jam):
        # make field types little less wrong
     
        corrector = {
            'state': 'string',
            'enum': 'string',
            'user': 'string',
            'float': 'number',
            'build': 'string',
            'period': 'number',
            'version': 'string',
            'ownedField': 'string',
            'EnumBundleElement': 'string'
        }
        
        # less correct :> more corrent
        for particular in jam.keys():
            _type = jam[particular]['type'][1]
            if _type in corrector: jam[particular]['type'][1] = corrector[_type]

        return jam


    @retry(requests.exceptions.ConnectionError, tries=config('TRIES'), delay=2)
    def parse_project_issues(self,project):
        # get all issues id's for project
        self.r = requests.get(self.baseUrl+'/admin/projects/'+project+'/issues?$top=-1', headers=self.headers)
        return [issue['id'] for issue in self.r.json()]


    @retry(requests.exceptions.ConnectTimeout, tries=config('TRIES'), delay=2)
    def transfer_issue(self,schema,id):           
        # main ETL loop for issue
        # get all necessary data for issue id
        fields = "id,idReadable,summary,project(name),created,resolved,updated,numberInProject"
        self.r = requests.get(self.baseUrl+'/issues/'+id+(
            '?fields='+fields+',customFields(name,value(name,value))'
            ), headers=self.headers, allow_redirects=True) 
        jas = json.loads(self.r.text)


        # prepare frame of non-custom fields
        res = {
            'timestamp': jas['created'], # used as target-postgres _seq_id
            'project': jas['project']['name'], 
            'id': jas['id'], 
            'summary': jas['summary'], 
            'idReadable': jas['idReadable'],
            'created': self.convert_ts(jas['created']),
            'updated': self.convert_ts(jas['updated']),
            'numberInProject': jas['numberInProject'],
            'resolved': self.convert_ts(jas['resolved'])
            }
        
        # add custom fields to fill up
        for item in jas['customFields']:
            res[item['name']] = item['value'] if type(item['value']) is not dict else item['value']['name']
            if type(item['value']) is not dict:
                res[item['name']] = item['value'] if item['value'] else None
            else:
                res[item['name']] = item['value']['name'] if item['value']['name'] else None

        # put jam in frame
        jam = {field:res[field] for field in schema['properties'].keys() if field in res.keys()}

        # write
        #singer.write_record('issue_'+project.replace('-', ''), jam)
        singer.write_record('issue', jam)


    @retry(requests.exceptions.ConnectTimeout, tries=config('TRIES'), delay=2)
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
            
            res['datetime'] = self.convert_ts(ch['timestamp'])
            
        # grind
        jam = {field:res[field] for field in self.HISTORY_SCHEMA['properties'].keys()}
    
        # write
        #ts = self.convert_ts(datetime.timestamp())
        singer.write_record('activity', jam)


def discover():
    yt = Connection(config('URL'), config('TOKEN'))
    map = yt.parse_fields_values_types(simplified=True)
    schema = yt.generate_schema(map,simplified=True)
    catalog = yt.make_catalog(schema)
    return catalog


@utils.handle_top_exception(LOGGER)
def run():
    
    yt = Connection(config('URL'), config('TOKEN'))
    
    # parse projects
    projects = yt.parse_projects()

    # parse fields map
    map = yt.parse_fields_values_types(simplified=True)

    # create activity stream
    singer.write_schema('activity', yt.HISTORY_SCHEMA, key_properties=['task_id'])

    # >>>>>
    for project in projects:

        if project in config('PROJECT_EXCLUDE', cast=Csv()): continue

        issues = yt.parse_project_issues(project)
        if len(issues) == 0: continue
        schema = yt.generate_schema(map, simplified=True)
        #singer.write_schema('issue_'+project.replace('-', ''), schema, 'id')
        singer.write_schema('issue', schema, key_properties=['id'])

        for issue in issues:
            yt.transfer_issue(schema,issue)
            if project in config('HISTORY_EXCLUDE', cast=Csv()): continue 
            yt.transfer_issue_activities(issue)


@utils.handle_top_exception(LOGGER)
def main():
    #Parse command line arguments
    args = utils.parse_args('')
    if args.discover:
        print(discover())
        LOGGER.info('Discover function used for observe catalog only')
    else:
        if args.catalog:
            return LOGGER.critical('Iterating through customized catalog does not supported.')
        run()


if __name__ == "__main__":
    main()
