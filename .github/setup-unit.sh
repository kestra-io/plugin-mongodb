docker compose -f docker-compose-ci.yml up -d
sleep 10
curl -s -o .github/workflows/mongo_data.jsonl https://raw.githubusercontent.com/ozlerhakan/mongodb-json-files/master/datasets/books.json
docker compose -f docker-compose-ci.yml exec mongo sh -c "mongoimport --authenticationDatabase admin -c books  "mongodb://root:example@localhost/samples" /tmp/docker/mongo_data.jsonl > /dev/null"
