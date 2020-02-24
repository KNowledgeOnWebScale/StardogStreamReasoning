FROM centos:7

# STARDOG_HOME is user data directory
ENV STARDOG_HOME=/stardog STARDOG_SERVER_JAVA_ARGS="-Xmx10g -Xms10g -XX:MaxDirectMemorySize=6g"

# Install stardog from its repo
RUN curl http://packages.stardog.com/rpms/stardog.repo > /etc/yum.repos.d/stardog.repo && \
    yum install -y stardog-7.0.2

# User data directory for $STARDOG_HOME
# VOLUME ["/stardog"]
# Potential volume for license key to be lifted at runtime
VOLUME ["/stardog-license"]

# Add the stardog zip file and license if present
COPY resources/stardog-license-key.bin /stardog-license/stardog-license-key.bin

EXPOSE 5820

# remove locks, always replace license key if present, start stardog
COPY ./script.sh /
RUN chmod +x /script.sh
ENTRYPOINT ["/script.sh"]
