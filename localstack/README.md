# Running
`docker-compose up` will start the stack with the services defined by `$LOCALSTACK_SERVICES` in the `docker-compose.yml` or `.env` overrides.

# Bootstrapping
Scripts are copied to `/opt/bootstrap/scripts`.

Templates are copied to `/opt/bootstrap/templates`.

By default the `init.sh` script creates an AWS stack using the CloudFormation template located in `/opt/bootstrap/templates`.

## Runtime overrides
Two options for overriding this at runtime:
- To just use a different CloudFormation template mount a Volume over `/opt/bootstrap/templates` containing a `cftemplate.yaml` template.
- To directly use `awslocal` on the Cli, mount a Volume over `/opt/bootstrap/scripts` containing an `init.sh` script.

[awslocal](https://github.com/localstack/awscli-local) is installed and used for bootstrapping scripts.

# Environment
The `.env` file will overwrite environment variables defined in the `docker-compose.yml`.

# Example
From your host, either install `awslocal` or pass the appropriate endpoint overrides to the aws Cli.

```bash
aws --endpoint-url=https://localhost:4568 kinesis --profile=personal --no-verify-ssl list-streams                                                   

/usr/local/Cellar/awscli/1.11.35/libexec/lib/python2.7/site-packages/botocore/vendored/requests/packages/urllib3/connectionpool.py:768: InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.org/en/latest/security.html
  InsecureRequestWarning)
{
    "StreamNames": [
        "int-test-stream-1",
        "int-test-stream-2"
    ]
}
```
