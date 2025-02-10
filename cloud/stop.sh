


# Current directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Stop the docker-compose services
docker-compose -f $DIR/docker-compose.yml down