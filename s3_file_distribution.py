#!/usr/bin/env python

from __future__ import print_function

import json
import urllib
import boto3

print("Loading function...")

s3 = boto3.resource("s3")

url = "https://s3-us-west-2.amazonaws.com/clavis-file-distribution-json/maps.json?version=3"
jsonurl = urllib.urlopen(url)
maps = json.loads(jsonurl.read())

# # For testing
# with open('event.json') as event_file:
#     event = json.load(event_file)
#
# with open('maps.json') as maps_file:
#     maps = json.load(maps_file)
#
def check_omit(key,omit):
    for s in omit:
        print("Check omit: " + s)
        if s in key:
            print("Omit match. Exiting.")
            return False
    return True

def s3_action(src_bucket,src_key,dst_bucket,dst_key):
    print("EVENT MATCHED. TRIGGERING FUNCTION.")
    try:
        response = s3.Object(dst_bucket],dst_key).copy_from(CopySource=src_bucket + '/' + src_key)
        # Testing
        # response = True
        print("Copy response is:")
        print(response)
        print("Original key is: " + src_bucket + "/" + src_key)
        print("Destination key is: " + dst_bucket + "/" + dst_key)
        return response
    except Exception as e:
        print(e)
        print('Error distributing ' + bucket + "/" + key)
        raise e


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

                print("Check string: " + map["string"] + " in " + key)

                if len(map["string"]) < 1:
                    print("No match string specified. Checking omissions.")
                    match = check_omit(key, map["omit"])

                    if match:
                        s3_action(map["src_bucket"], key, map["dst_bucket"], map["dst_path"] + key_file)

                else:
                    if map["string"] in key_file:
                        print("String matched. Checking omissions.")
                        match = check_omit(key,map["omit"])

                        if match:
                            s3_action(map["src_bucket"], key, map["dst_bucket"], map["dst_path"] + key_file)

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

# #Testing only
# context = False
# lambda_handler(event, context)