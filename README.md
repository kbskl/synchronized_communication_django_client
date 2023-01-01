# What is this?

This repo contains the server codes with MultipleServerManager which should be on multiple django servers in a concurrent communication structure.


## Installation

1. Clone the repository
2. Install Redis in your computer and start Redis
3. Install PostgreSQL in your computer and start PostgreSQL
4. Install required packages
   ```
   pip3 install -r requirements.txt
   ```
5. Enter the PostgreSQL and Redis settings you installed into the .env file
6. Enter the Redis settings you installed into the multiple_server_manager_config.py file



## Usage

You can review the document I described for use.<br>
[Medium Document](https://tarkkabasakal.medium.com/python-django-ile-channels-kullan%C4%B1m%C4%B1-6797928087b8)



## License
[MIT](https://choosealicense.com/licenses/mit/)
