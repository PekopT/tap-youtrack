#!/usr/bin/env python3
import os
import json
import singer
import requests
from retry import retry
from singer import utils, metadata
#from singer.catalog import Catalog, CatalogEntry
#from singer.schema import Schema
from decouple import config, Csv


REQUIRED_CONFIG_KEYS = []
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

    #@retry(requests.exceptions.ConnectionError, tries=3, delay=2)
    def parse_projects(self):
        # get all project database id's
        self.r = requests.get(self.baseUrl+'/admin/projects/?fields=name,id', headers=self.headers)
        return {project['id']: project['name'] for project in self.r.json()}

    #@retry(requests.exceptions.ConnectionError, tries=5, delay=2)
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
            'additionalProperties': False,
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
        jam['project'] = {'type':['string']} 
        jam['id'] = {'type':['string']}
        jam['summary'] = {'type':['string']}
        
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
                                "version": "1.0.0"
                            }
                        }
                    ]
                },
                {
                    "stream": 'activity',
                    "tap_stream_id": 'activity',
                    "schema": 'schema',
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
                                        "primary_key": True if name == "id" else False
                                    } for name, types in self.HISTORY_SCHEMA['properties'].items()
                                ],
                                "key_properties": [
                                    "task_id"
                                ],
                                "incremental": True,
                                "replication_method": "INCREMENTAL",
                                "version": "1.0.0"
                            }
                        }
                    ]
                }
            ]
        }
        return json.dumps(catalog,indent=4)

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


    #@retry(requests.exceptions.ConnectionError, tries=5, delay=2)
    def parse_project_issues(self,project):
        # get all issues id's for project
        self.r = requests.get(self.baseUrl+'/admin/projects/'+project+'/issues?$top=-1', headers=self.headers)
        return [issue['id'] for issue in self.r.json()]

    @retry(requests.exceptions.ConnectTimeout, tries=5, delay=2)
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
        jam = {field:res[field] for field in schema['properties'].keys() if field in res.keys()}

        # write
        #singer.write_record('issue_'+project.replace('-', ''), jam)
        singer.write_record('issue', jam)

    @retry(requests.exceptions.ConnectTimeout, tries=5, delay=2)
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



def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def discover():
    yt = Connection('https://singer-export.youtrack.cloud/youtrack/', config('TOKEN'))
    map = yt.parse_fields_values_types(simplified=True)
    schema = yt.generate_schema(map,simplified=True)
    catalog = yt.make_catalog(schema)
    return catalog


'''
def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema,
            key_properties=stream.key_properties,
        )

        # TODO: delete and replace this inline function with your own data retrieval process:
        tap_data = lambda: [{"id": x, "name": "row${x}"} for x in range(1000)]

        max_bookmark = None
        for row in tap_data():
            # TODO: place type conversions or transformations here

            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [row])
            if bookmark_column:
                if is_sorted:
                    # update bookmark to latest value
                    singer.write_state({stream.tap_stream_id: row[bookmark_column]})
                else:
                    # if data unsorted, save max value until end of writes
                    max_bookmark = max(max_bookmark, row[bookmark_column])
        if bookmark_column and not is_sorted:
            singer.write_state({stream.tap_stream_id: max_bookmark})
    return
'''

@utils.handle_top_exception(LOGGER)
def run():
    
    yt = Connection('https://singer-export.youtrack.cloud/youtrack/', config('TOKEN'))
    
    # parse projects
    projects = yt.parse_projects()

    # parse fields map
    map = yt.parse_fields_values_types(simplified=True)

    # create activity stream
    singer.write_schema('activity', yt.HISTORY_SCHEMA,'')

    # >>>>>
    for project in projects:

        if project in config('PROJECT_EXCLUDE', cast=Csv()): continue

        issues = yt.parse_project_issues(project)
        if len(issues) == 0: continue
        schema = yt.generate_schema(map, simplified=True)
        #singer.write_schema('issue_'+project.replace('-', ''), schema, 'id')
        singer.write_schema('issue', schema, 'id')

        for issue in issues:
            yt.transfer_issue(project,schema,issue)
            if project in config('HISTORY_EXCLUDE', cast=Csv()): continue 
            yt.transfer_issue_activities(issue)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        print(discover())
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        #sync(args.config, args.state, catalog)
        run()


if __name__ == "__main__":
    # get connection
    main()
