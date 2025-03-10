# Build docker images
docker build --pull --rm -f 'docker/analytics/Dockerfile' -t 'doreisa-analytics:latest' 'docker/analytics'
docker build --pull --rm -f 'docker/simulation/Dockerfile' -t 'doreisa-simulation:latest' 'docker/simulation'

# Export the docker images to a .tar file
docker save doreisa-analytics:latest -o docker/images/doreisa-analytics.tar
docker save doreisa-simulation:latest -o docker/images/doreisa-simulation.tar

# Convert the images to singularity images
singularity build docker/images/doreisa-analytics.sif docker-archive://docker/images/doreisa-analytics.tar
singularity build docker/images/doreisa-simulation.sif docker-archive://docker/images/doreisa-simulation.tar
