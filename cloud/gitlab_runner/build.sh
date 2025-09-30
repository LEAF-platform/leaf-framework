# Get current direcotry
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"                            
cd $DIR
cp $DIR/../../pyproject.toml $DIR/

docker login registry.gitlab.com

docker build --platform linux/amd64 -t registry.gitlab.com/labequipmentadapterframework/leaf:runner .
docker push registry.gitlab.com/labequipmentadapterframework/leaf:runner

docker build --platform linux/arm64 -t registry.gitlab.com/labequipmentadapterframework/leaf:runner .
docker push registry.gitlab.com/labequipmentadapterframework/leaf:runner