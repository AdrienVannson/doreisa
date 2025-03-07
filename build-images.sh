# Build docker images
docker build --pull --rm -f 'docker/analytics/Dockerfile' -t 'doreisa-analytics:latest' 'docker/analytics'
docker build --pull --rm -f 'docker/simulation/Dockerfile' -t 'doreisa-simulation:latest' 'docker/simulation'

# Export the docker images to a .tar file
mkdir -p docker/images
docker save doreisa-analytics:latest -o dcker/images/doreisa-analytics.tar
docker save doreisa-simulation:latest -o docker/images/doreisa-simulation.tar