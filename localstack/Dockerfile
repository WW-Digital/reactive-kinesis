ARG  LOCALSTACK_DOCKER_IMAGE_TAG=latest
FROM localstack/localstack:$LOCALSTACK_DOCKER_IMAGE_TAG

COPY bootstrap /opt/bootstrap/

# Append our init script as a program in the supervisord config
RUN cat /opt/bootstrap/supervisord-init.conf >> /etc/supervisord.conf

RUN pip install awscli-local 

RUN chmod +x /opt/bootstrap/scripts/init.sh
