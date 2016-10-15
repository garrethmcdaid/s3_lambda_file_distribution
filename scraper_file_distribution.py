#!/usr/bin/env python

from __future__ import print_function

import json
import urllib
import boto3

print('Loading function')

s3 = boto3.resource('s3')

url = "http://localhost/maps.json?version=1"
jsonurl = urllib.urlopen(url)
maps = json.loads(jsonurl.read())

# # For testing
# with open('event.json') as event_file:
#     event = json.load(event_file)
#
# with open('maps.json') as maps_file:
#     maps = json.load(maps_file)

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    if event['Records'][0]['s3']['object']['size'] < 1:
        print("Directory creation. No action required.")
        return

    # Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).decode('utf8')
    size = event['Records'][0]['s3']['object']['size']

    #Print NAGIOS string so that Nagios can monitor S3 logs in Elasticsearch
    print("NAGIOS|" + key + "|" + str(size) + "|" + event['Records'][0]['eventTime'])

    for map in maps["maps"]:
        try:
            key_split = key.split("/")
            key_file = key_split[-1]
            print("Check bucket and path: " + map["src_bucket"] + " " + map["src_path"])
            if map["src_bucket"] == bucket and map["src_path"] in key:
                print("Check string: " + map["string"])

                if map["string"] in key_file:
                    match = True
                    for s in map["omit"]:
                        print("Check omit: " + s)
                        if s in key_file:
                            match = False
                            print("Omit match")
                            break
                    if match:
                        print("EVENT MATCH")
                        try:
                            dst_key = map["dst_path"] + key_file
                            response = s3.Object(map["dst_bucket"],dst_key).copy_from(CopySource=bucket + '/' + key)
                            print("Copy response is:")
                            print(response)
                            print("Original key is: " + key)
                            print("Destination key is: " + map["dst_bucket"] + "/" + map["dst_path"] + key_file)

                            #return response
                        except Exception as e:
                            print(e)
                            print('Error distributing ' + bucket + "/" + key)
                            raise e
                else:
                    print("String not matched. Function not triggered.")


            else:
                print("Map: " + map["src_bucket"] + " Event: " + bucket)
                print("Map: " + map["src_path"] + " Event: " + key)
                print("Bucket and dst not matched. Function not triggered.")
        except Exception as e:
            print('Error parsing key details')
            print(e)
            raise e