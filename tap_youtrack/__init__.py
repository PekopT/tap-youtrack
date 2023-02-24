#!/usr/bin/env python3
# pylint: disable=broad-except,chained-comparison,empty-docstring,fixme,invalid-name,line-too-long,missing-class-docstring,missing-function-docstring,missing-module-docstring,no-else-raise,no-else-return,too-few-public-methods,too-many-arguments
# pylint: disable=no-value-for-parameter

import json
import time
from datetime import datetime
from requests import exceptions as exc
import requests
import singer
from pytz import UTC
from retry import retry
from singer import utils
from decouple import config, Csv

LOGGER = singer.get_logger()
TRIES = int(config("TRIES"))


class Connection:
    def __init__(self, url, token):
        self.url = url.rstrip("/")
        self.base_url = self.url + "/api"
        self.headers = {
            "Authorization": "Bearer " + token,
            "Accept": "application/json",
        }
        self.session = requests.Session()

        self.history_schema = {
            "type": ["null", "object"],
            "additionalProperties": True,
            "properties": {
                "id": {"type": "string"},
                "task_id": {"type": ["null", "string"]},
                "author": {"type": ["null", "string"]},
                "field": {"type": ["null", "string"]},
                "prev_state": {"type": ["null", "string"]},
                "state": {"type": ["null", "string"]},
                "datetime": {"format": "date-time", "type": ["null", "string"]},
                "timestamp": {"type": ["number"]},
            },
        }

        self.link_schema = {
            "type": ["null", "object"],
            "additionalProperties": True,
            "properties": {
                "id": {"type": "string"},
                "origin": {"type": "string"},
                "link_type": {"type": "string"},
                "relative": {"type": ["string", "array"]},
            },
        }

    @retry((exc.ConnectTimeout, exc.ConnectionError), tries=TRIES, delay=2)
    def parse_projects(self):
        # get all project database id's
        r = self.session.get(
            self.base_url + "/admin/projects/?fields=name,id",
            headers=self.headers,
            timeout=5,
        )
        if r.status_code == 200:
            LOGGER.info("Successful API auth")
        return {project["id"]: project["name"] for project in r.json()}

    @retry((exc.ConnectTimeout, exc.ConnectionError), tries=TRIES, delay=2)
    def parse_fields_values_types(self):
        # get all fields with data types, names, and project instances
        request = (
            "customFields?fields=fieldType(type,valueType),instances(project(id)),name"
        )
        r = self.session.get(
            self.base_url + "/admin/customFieldSettings/" + request,
            headers=self.headers,
            timeout=5,
        )
        # before - generate default fields items
        a = {
            field["name"]: [field["fieldType"]["valueType"], field["instances"]]
            for field in r.json()
        }
        b = {}  # mapping below >> {project_id:[{field_name:field_type}]}

        for name, values in a.items():  # if field have any instance
            b[name] = values[0]
        return b

    def generate_schema(self, types_map):
        # build a schema for data stream
        schema = {
            "type": ["null", "object"],
            "additionalProperties": True,
            "properties": {
                #  << putting jam here
            },
        }

        # custom fields from map
        raw_jam = {key: {"type": ["null", value]} for key, value in types_map.items()}

        # grind data types for custom fields
        jam = self.convert_data_types(raw_jam)

        # non-custom fields
        jam["timestamp"] = {"type": ["number"]}  # using as _seq_id to target-postgres
        jam["project"] = {"type": ["string"]}
        jam["project_id"] = {"type": ["string"]}
        jam["project_short_name"] = {"type": ["string"]}
        jam["id"] = {"type": ["string"]}
        jam["summary"] = {"type": ["string"]}
        jam["idReadable"] = {"type": ["string"]}
        jam["created"] = {"type": ["null", "string"], "format": "date-time"}
        jam["resolved"] = {"type": ["null", "string"], "format": "date-time"}
        jam["updated"] = {"type": ["null", "string"], "format": "date-time"}
        jam["numberInProject"] = {"type": ["number"]}

        # wrap fields in schema template
        schema["properties"] = jam

        return schema

    def make_catalog(self, schema):
        # Generate the catalog based on the retrieved schema information

        catalog = {
            "streams": [
                {
                    "stream": "issue",
                    "tap_stream_id": "issue",
                    "schema": schema,
                    "metadata": [
                        {
                            "breadcrumb": [],
                            "metadata": {
                                "table_name": "issue",
                                "schema": "public",
                                "columns": [
                                    {
                                        "name": name,
                                        "type": types,
                                        "primary_key": name == "id",
                                    }
                                    for name, types in schema["properties"].items()
                                ],
                                "key_properties": ["id"],
                                "incremental": True,
                                "replication_method": "INCREMENTAL",
                                "version": "1.0.0",
                                "sequence": "timestamp",
                            },
                        }
                    ],
                },
                {
                    "stream": "activity",
                    "tap_stream_id": "activity",
                    "schema": self.history_schema,
                    "metadata": [
                        {
                            "breadcrumb": [],
                            "metadata": {
                                "table_name": "activity",
                                "schema": "public",
                                "columns": [
                                    {
                                        "name": name,
                                        "type": types,
                                        "primary_key": name == "id",
                                    }
                                    for name, types in self.history_schema[
                                        "properties"
                                    ].items()
                                ],
                                "key_properties": ["id"],
                                "incremental": True,
                                "replication_method": "INCREMENTAL",
                                "version": "1.0.0",
                                "sequence": "timestamp",
                            },
                        }
                    ],
                },
            ]
        }
        return json.dumps(catalog, indent=4)

    def convert_ts(self, ts):
        # from UNIX timestamp to reccommended by singer format
        if ts is not None:
            dt = UTC.localize(datetime.utcfromtimestamp(ts / 1000))
            return dt.isoformat("T")
        return None

    def convert_data_types(self, jam):
        # make field types little less wrong

        corrector = {
            "state": "string",
            "enum": "string",
            "user": "string",
            "text": "string",
            "date": "number",
            "float": "number",
            "build": "string",
            "period": "number",
            "version": "string",
            "ownedField": "string",
            "EnumBundleElement": "string",
        }

        # less correct :> more corrent
        for particular in jam.keys():
            _type = jam[particular]["type"][1]
            if _type in corrector:
                jam[particular]["type"][1] = corrector[_type]

        return jam

    @retry((exc.ConnectTimeout, exc.ConnectionError), tries=TRIES, delay=2)
    def transfer_link(self, issue):
        r = self.session.get(
            self.base_url
            + "/issues/"
            + issue
            + "/links/"
            + "?fields=id,direction,linkType(sourceToTarget,targetToSource),issues(id)",
            headers=self.headers,
            allow_redirects=True,
            timeout=5,
        )
        for link in r.json():
            if link["issues"]:
                jam = {
                    "id": link["id"],
                    "origin": issue,
                    "link_type": link["linkType"]["sourceToTarget"]
                    if link["direction"] != "INWARD"
                    else link["linkType"]["targetToSource"],
                    "relative": ",".join([l["id"] for l in link["issues"]]),
                }
                jam = {field: jam[field] for field in self.link_schema["properties"]}
                singer.write_record("link", jam)

    @retry((exc.ConnectTimeout, exc.ConnectionError), tries=TRIES, delay=2)
    def parse_project_issues(self, project, skip):
        # get all issues id's for project
        top = config("BATCH_SIZE")
        r = self.session.get(
            self.base_url
            + "/admin/projects/"
            + project
            + "/issues?$skip="
            + str(skip)
            + "&$top="
            + top,
            headers=self.headers,
            timeout=5,
        )
        if len(r.json()) > 0:
            LOGGER.info(
                "Recieved batch of %s issues for %s, transferring...",
                len(r.json()),
                project,
            )
        return [issue["id"] for issue in r.json()]

    def root_transfer(self, project, schema):
        a, i, b = 0, 0, 0  # activities, issues, batches
        completed = False
        batch_size = int(config("BATCH_SIZE"))
        batch_delay = int(config("BATCH_DELAY"))

        while not completed:
            i = 0
            issues = self.parse_project_issues(project, b * batch_size)  # get new batch
            if not issues and not b:
                return LOGGER.info(
                    "Skipping empty project: %s", project
                )  # skip if empty

            for issue in issues:
                i += 1
                self.transfer_issue(schema, issue)
                self.transfer_link(issue)
                if project not in config("HISTORY_EXCLUDE", cast=Csv()):
                    a += self.transfer_issue_activities(issue)

            if batch_delay and i == batch_size:  # if we had any transfered issue
                LOGGER.info(
                    "Batch iteration #%s done for %s. Overall parsed: %s.",
                    b + 1,
                    project,
                    b * batch_size + i,
                )
                LOGGER.info("Cooling down %ssec.", batch_delay)
                time.sleep(batch_delay)

            if len(issues) < batch_size:  # complete if batch was not full
                completed = True
            else:
                b += 1
                if batch_delay:
                    LOGGER.info("Continue")

        LOGGER.info(
            "Parsing of project %s completed, overall tasks: %s, activities: %s \n",
            project,
            b * batch_size + i,
            a,
        )

    @retry((exc.ConnectTimeout, exc.ConnectionError), tries=TRIES, delay=2)
    def transfer_issue(self, schema, iid):
        # main ETL loop for issue
        # get all necessary data for issue id
        fields = "id,idReadable,summary,project(id,name,shortName),created,resolved,updated,numberInProject"
        r = self.session.get(
            self.base_url
            + "/issues/"
            + iid
            + ("?fields=" + fields + ",customFields(name,value(name,value))"),
            headers=self.headers,
            allow_redirects=True,
            timeout=5,
        )
        jas = json.loads(r.text)

        # prepare frame of non-custom fields
        res = {
            "timestamp": jas["created"],  # used as target-postgres _seq_id
            "project": jas["project"]["name"],
            "project_id": jas["project"]["id"],
            "project_short_name": jas["project"]["shortName"],
            "id": jas["id"],
            "summary": jas["summary"],
            "idReadable": jas["idReadable"],
            "created": self.convert_ts(jas["created"]),
            "updated": self.convert_ts(jas["updated"]),
            "numberInProject": jas["numberInProject"],
            "resolved": self.convert_ts(jas["resolved"]),
        }

        # add custom fields to fill up
        try:
            for item in jas["customFields"]:
                if isinstance(item["value"], list):
                    res[item["name"]] = ",".join([x["name"] for x in item["value"]])
                elif isinstance(item["value"], dict):
                    res[item["name"]] = (
                        item["value"]["name"]
                        if ("name" in item["value"] and item["value"]["name"])
                        else None
                    )
                else:
                    res[item["name"]] = item["value"] if item["value"] else None

            # put jam in frame
            jam = {
                field: res[field]
                for field in schema["properties"].keys()
                if field in res
            }

            # write
            singer.write_record("issue", jam)
        except Exception as Ex:
            LOGGER.error("Exception: %s raised for record: %s jam", Ex, jam)

    @retry((exc.ConnectTimeout, exc.ConnectionError), tries=TRIES, delay=2)
    def transfer_issue_activities(self, iid):
        # main ETL loop for issue history
        res = {}
        # get any history data for fields changes of issue id
        fields = (
            "fields=id,field(name),author(login),timestamp,added(name),removed(name)"
        )
        categories = "categories=CustomFieldCategory,IssueResolvedCategory,ProjectCategory,ProjectCategory,SummaryCategory"
        r = self.session.get(
            self.base_url
            + "/issues/"
            + iid
            + "/activities?"
            + categories
            + "&"
            + fields,
            headers=self.headers,
            timeout=5,
        )
        changes = r.json()
        if not changes:
            return False

        # read
        for ch in changes:
            res["id"] = ch["id"]
            res["task_id"] = iid
            res["author"] = ch["author"]["login"]
            res["field"] = ch["field"]["name"]
            res["state"] = (
                str(ch["added"])
                if not isinstance((ch["added"]), list)
                else ",".join([x["name"] for x in ch["added"]])
            )
            res["prev_state"] = (
                str(ch["removed"])
                if not isinstance((ch["removed"]), list)
                else ",".join([x["name"] for x in ch["removed"]])
            )
            res["timestamp"] = ch["timestamp"]
            res["datetime"] = self.convert_ts(ch["timestamp"])

            # grind
            jam = {field: res[field] for field in self.history_schema["properties"]}

            # write
            singer.write_record("activity", jam)

        return len(changes)


def discover():
    yt = Connection(config("URL"), config("TOKEN"))
    types_map = yt.parse_fields_values_types()
    schema = yt.generate_schema(types_map)
    catalog = yt.make_catalog(schema)
    return catalog


@utils.handle_top_exception(LOGGER)
def run():
    yt = Connection(config("URL"), config("TOKEN"))

    # parse projects
    projects = yt.parse_projects()

    # parse fields map
    types_map = yt.parse_fields_values_types()

    # create activity & link streams
    singer.write_schema("activity", yt.history_schema, key_properties=["id"])
    singer.write_schema("link", yt.link_schema, key_properties=["origin","link_type","relative"])

    # >>>>>
    for project in projects:
        if project in config("PROJECT_EXCLUDE", cast=Csv()):
            continue

        schema = yt.generate_schema(types_map)
        singer.write_schema("issue", schema, key_properties=["id"])
        yt.root_transfer(project, schema)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args("")
    if args.discover:
        print(discover())
        LOGGER.info("Discover function used for observe catalog only")
    else:
        if args.catalog:
            LOGGER.critical("Iterating through customized catalog does not supported.")
            return
        run()


if __name__ == "__main__":
    main()
