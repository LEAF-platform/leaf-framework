docker login docker-registry.wur.nl

docker build --platform linux/amd64 -t docker-registry.wur.nl/m-unlock/docker/leaf:dev .

docker push docker-registry.wur.nl/m-unlock/docker/leaf:dev