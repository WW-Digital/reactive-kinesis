#!/usr/bin/env bash

awslocal cloudformation create-stack --template-body file:///opt/bootstrap/templates/cftemplate.yml --stack-name teststack


# :D

# bash-4.3# awslocal cloudformation create-stack --template-body file://cftemplate.yaml --stack-name teststack4
# {
#     "StackId": "arn:aws:cloudformation:us-east-1:123456789:stack/teststack4/4170cb26-6f7b-47e5-a30a-d229e22e2ff0"
# }
# bash-4.3# awslocal kinesis list-streams
# {
#     "StreamNames": [
#         "cf-test-stream-1",
#         "cf-test-stream-12",
#         "cf-test-stream-6",
#         "int-test-stream-1",
#         "int-test-stream-2"
#     ]
# }
# bash-4.3# awslocal kinesis put-record --stream-name local.core-notifications-service.1.Event --data '{"headers":{"messageId":"e9d9413f-84f2-11e6-9f2b-0fa4680d320a","source":"test source","action":"Create"},"payload":"\n{\n\t\"userId\": \"82d5774f-79ed-43bc-baad-6592bd06c3cf\",\n\t\"eventId\": \"443fc0e2-ecd6-41cb-8ec7-10de2788179b\",\n\t\"targetId\": \"some-target-id\",\n\t\"eventType\": \"like_a_post\",\n\t\"eventCategory\": \"Connect\",\n\t\"locale\": \"en_US\",\n\t\"eventTime\": \"2016-09-27T20:42:31.998Z\",\n\t\"priority\": 3,\n\t\"path\": \"http://meh.com\",\n\t\"spiceMeta\": {\n\t\t\"name\": \"fred\"\n\t},\n\t\"image\": \"image path\",\n\t\"actioned\": \"2016-09-27T20:42:32.073Z\",\n\t\"isPush\": false,\n\t\"isSilent\": true,\n\t\"ttlSeconds\": 123\n}\n "}' --partition-key 31c5f564-7aa8-11e6-b113-07e4959eb065

# An error occurred (ResourceNotFoundException) when calling the PutRecord operation: Stream local.core-notifications-service.1.Event under account 000000000000 not found.
# bash-4.3# awslocal kinesis put-record --stream-name int-test-stream-1 --data '{"headers":{"messageId":"e9d9413f-84f2-11e6-9f2b-0fa4680d320a","source":"test source","action":"Create"},"payload":"\n{\n\t\"userId\": \"82d5774f-79ed-43bc-baad-6592bd06c3cf\",\n\t\"eventId\": \"443fc0e2-ecd6-41cb-8ec7-10de2788179b\",\n\t\"targetId\": \"some-target-id\",\n\t\"eventType\": \"like_a_post\",\n\t\"eventCategory\": \"Connect\",\n\t\"locale\": \"en_US\",\n\t\"eventTime\": \"2016-09-27T20:42:31.998Z\",\n\t\"priority\": 3,\n\t\"path\": \"http://meh.com\",\n\t\"spiceMeta\": {\n\t\t\"name\": \"fred\"\n\t},\n\t\"image\": \"image path\",\n\t\"actioned\": \"2016-09-27T20:42:32.073Z\",\n\t\"isPush\": false,\n\t\"isSilent\": true,\n\t\"ttlSeconds\": 123\n}\n "}' --partition-key 31c5f564-7aa8-11e6-b113-07e4959eb065
# {
#     "ShardId": "shardId-000000000000",
#     "SequenceNumber": "49576854861875158109182690829527624497638593274623033346"
# }
# bash-4.3#



# From HOST:
# aws --endpoint-url=https://localhost:4568 kinesis --profile=personal --no-verify-ssl put-record --stream-name int-test-stream-1 --data '{"headers":{"messageId":"e9d9413f-84f2-11e6-9f2b-0fa4680d320a","source":"test source","action":"Create"},"payload":"\n{\n\t\"userId\": \"82d5774f-79ed-43bc-baad-6592bd06c3cf\",\n\t\"eventId\": \"443fc0e2-ecd6-41cb-8ec7-10de2788179b\",\n\t\"targetId\": \"some-target-id\",\n\t\"eventType\": \"like_a_post\",\n\t\"eventCategory\": \"Connect\",\n\t\"locale\": \"en_US\",\n\t\"eventTime\": \"2016-09-27T20:42:31.998Z\",\n\t\"priority\": 3,\n\t\"path\": \"http://meh.com\",\n\t\"spiceMeta\": {\n\t\t\"name\": \"fred\"\n\t},\n\t\"image\": \"image path\",\n\t\"actioned\": \"2016-09-27T20:42:32.073Z\",\n\t\"isPush\": false,\n\t\"isSilent\": true,\n\t\"ttlSeconds\": 123\n}\n "}' --partition-key 31c5f564-7aa8-11e6-b113-07e4959eb065
# /usr/local/Cellar/awscli/1.11.35/libexec/lib/python2.7/site-packages/botocore/vendored/requests/packages/urllib3/connectionpool.py:768: InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.org/en/latest/security.html
#   InsecureRequestWarning)
# {
#     "ShardId": "shardId-000000000000",
#     "SequenceNumber": "49576877379205194007935244315840793009965350368929906690"
# }

# aws --endpoint-url=https://localhost:4568 kinesis --profile=personal --no-verify-ssl get-shard-iterator --shard-id shardId-000000000000 --shard-iterator-type LATEST --stream-name int-test-stream-1

# aws --endpoint-url=https://localhost:4568 kinesis --profile=personal --no-verify-ssl get-records --shard-iterator AAAAAAAAAAHEAfAsvw8ElmygZq3XbhGqsK5uUfhOr8MdRdoi9jdoZMn+LSD4l6r5+86zJ9qSYARw9/AeOYOS4XI81cmaFvcbZyN/cMCywd6Xjgdg3UcyXYXen7Wx67ZyJWPZyVt8AGKNcpmpH8mHp79td43GcmpfF4+FksPIrHi2sGT/S1ri8xDHqN7HVcTkKgxrH+T0a967rhyRZDCoJ+i0cOlOAfi8
# /usr/local/Cellar/awscli/1.11.35/libexec/lib/python2.7/site-packages/botocore/vendored/requests/packages/urllib3/connectionpool.py:768: InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.org/en/latest/security.html
#   InsecureRequestWarning)
# {
#     "Records": [
#         {
#             "Data": "eyJoZWFkZXJzIjp7Im1lc3NhZ2VJZCI6ImU5ZDk0MTNmLTg0ZjItMTFlNi05ZjJiLTBmYTQ2ODBkMzIwYSIsInNvdXJjZSI6InRlc3Qgc291cmNlIiwiYWN0aW9uIjoiQ3JlYXRlIn0sInBheWxvYWQiOiJcbntcblx0XCJ1c2VySWRcIjogXCI4MmQ1Nzc0Zi03OWVkLTQzYmMtYmFhZC02NTkyYmQwNmMzY2ZcIixcblx0XCJldmVudElkXCI6IFwiNDQzZmMwZTItZWNkNi00MWNiLThlYzctMTBkZTI3ODgxNzliXCIsXG5cdFwidGFyZ2V0SWRcIjogXCJzb21lLXRhcmdldC1pZFwiLFxuXHRcImV2ZW50VHlwZVwiOiBcImxpa2VfYV9wb3N0XCIsXG5cdFwiZXZlbnRDYXRlZ29yeVwiOiBcIkNvbm5lY3RcIixcblx0XCJsb2NhbGVcIjogXCJlbl9VU1wiLFxuXHRcImV2ZW50VGltZVwiOiBcIjIwMTYtMDktMjdUMjA6NDI6MzEuOTk4WlwiLFxuXHRcInByaW9yaXR5XCI6IDMsXG5cdFwicGF0aFwiOiBcImh0dHA6Ly9tZWguY29tXCIsXG5cdFwic3BpY2VNZXRhXCI6IHtcblx0XHRcIm5hbWVcIjogXCJmcmVkXCJcblx0fSxcblx0XCJpbWFnZVwiOiBcImltYWdlIHBhdGhcIixcblx0XCJhY3Rpb25lZFwiOiBcIjIwMTYtMDktMjdUMjA6NDI6MzIuMDczWlwiLFxuXHRcImlzUHVzaFwiOiBmYWxzZSxcblx0XCJpc1NpbGVudFwiOiB0cnVlLFxuXHRcInR0bFNlY29uZHNcIjogMTIzXG59XG4gIn0=",
#             "PartitionKey": "31c5f564-7aa8-11e6-b113-07e4959eb065",
#             "ApproximateArrivalTimestamp": 1505051578.963,
#             "SequenceNumber": "49576877379205194007935244315842001935784985270350249986"
#         }
#     ],
#     "NextShardIterator": "AAAAAAAAAAG4/udZ//dA3V8bKc9i5mOtjocDKMJZr97nb5OerLN3kCc1lyINnYVp80c0ii6WJK2DP2wBhMvq1kc4QpG5VfY21KcmPBxEO3Ef6X+YwkBZE+ZzqNhURcoSHDJ9pdZMYXDX3ctcc+jd9sa+Dss7J4WBc7+CAzQZrko5qY3ZdR2suNBzvr434CQMMKmvKXcUXhdcDrSuA2t+if71b09IWo1I",
#     "MillisBehindLatest": 0
# }
