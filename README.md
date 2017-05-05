1. Installing Docker Compose
 https://docs.docker.com/compose/install/
2. Run app
- Go to root dir: cd etl_project
- Put your input data into "input" folder
- Build and run containers: docker-compose run etl bash
- Run program: python src/main.py ../app/input/your_data.txt
3. Check results
- Go to root dir: cd etl_project
- Find name of the runnig container with mongo: docker ps | grep "mongo"
- Start mongodb shell in container (name is etlproject_db_1): docker exec -it etlproject_db_1 mongo target_database
- output existing tables: db.getCollectionNames()
- output collection content: db.hitter.find()
