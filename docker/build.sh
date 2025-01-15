docker login docker-registry.wur.nl

docker build --no-cache --platform linux/amd64 -t docker-registry.wur.nl/m-unlock/docker/leaf:dev .

docker push docker-registry.wur.nl/m-unlock/docker/leaf:dev